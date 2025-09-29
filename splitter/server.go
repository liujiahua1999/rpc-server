package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// Job structure (matches worker)
type Job struct {
	JobID   string `json:"job_id"`
	PartNum int    `json:"part_num"`
	Content string `json:"content"`
}

// Result structure (matches worker)
type JobResult struct {
	JobID       string         `json:"job_id"`
	PartNum     int            `json:"part_num"`
	WorkerID    string         `json:"worker_id"`
	WordCount   map[string]int `json:"word_count"`
	TotalWords  int            `json:"total_words"`
	UniqueWords int            `json:"unique_words"`
	Status      string         `json:"status"`
}

// RPC structures (matches worker)
type GetJobArgs struct {
	WorkerID string
}

type GetJobReply struct {
	Job    Job
	HasJob bool
}

// Server state management
type MapReduceServer struct {
	mu               sync.RWMutex
	jobs             []Job
	pendingJobs      []Job
	completedResults map[string]JobResult
	finalWordCount   map[string]int
	totalProcessed   int
	totalJobs        int
	jobInProgress    map[string]time.Time // jobID -> start time
	maxRetries       int
	retryTimeout     time.Duration
}

// JobServer RPC service
type JobServer struct {
	server *MapReduceServer
}

// GetJob RPC method - called by workers to get work
func (js *JobServer) GetJob(args *GetJobArgs, reply *GetJobReply) error {
	js.server.mu.Lock()
	defer js.server.mu.Unlock()

	log.Printf("Worker %s requesting job", args.WorkerID)

	// Check for timed out jobs and re-queue them
	js.server.requeueTimedOutJobs()

	// Check if we have pending jobs
	if len(js.server.pendingJobs) == 0 {
		reply.HasJob = false
		log.Printf("No jobs available for worker %s", args.WorkerID)
		return nil
	}

	// Give the first pending job to the worker
	job := js.server.pendingJobs[0]
	js.server.pendingJobs = js.server.pendingJobs[1:]

	// Track this job as in progress
	jobKey := fmt.Sprintf("%s-%d", job.JobID, job.PartNum)
	js.server.jobInProgress[jobKey] = time.Now()

	reply.Job = job
	reply.HasJob = true

	log.Printf("Assigned job %s part %d to worker %s", job.JobID, job.PartNum, args.WorkerID)
	return nil
}

// Requeue jobs that have been in progress too long
func (mrs *MapReduceServer) requeueTimedOutJobs() {
	now := time.Now()
	for jobKey, startTime := range mrs.jobInProgress {
		if now.Sub(startTime) > mrs.retryTimeout {
			log.Printf("Job %s timed out, re-queuing", jobKey)

			// Find the original job and re-queue it
			parts := strings.Split(jobKey, "-")
			if len(parts) >= 2 {
				jobID := strings.Join(parts[:len(parts)-1], "-")
				partNum, err := strconv.Atoi(parts[len(parts)-1])
				if err == nil {
					// Find the job in our original jobs list
					for _, job := range mrs.jobs {
						if job.JobID == jobID && job.PartNum == partNum {
							mrs.pendingJobs = append(mrs.pendingJobs, job)
							break
						}
					}
				}
			}
			delete(mrs.jobInProgress, jobKey)
		}
	}
}

// Download file from URL
func downloadFromURL(url string) (string, error) {
	log.Printf("Downloading file from: %s", url)

	// Create HTTP client with timeout
	client := &http.Client{
		Timeout: 30 * time.Second,
	}

	// Make HTTP GET request
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %v", err)
	}
	defer resp.Body.Close()

	// Check status code
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: HTTP %d", resp.StatusCode)
	}

	// Read the content
	content, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to read file content: %v", err)
	}

	log.Printf("Successfully downloaded %d bytes", len(content))
	return string(content), nil
}

// Split content into chunks for parallel processing
func splitContentIntoJobs(content string, jobID string, chunkSize int) []Job {
	var jobs []Job
	contentRunes := []rune(content)
	totalLength := len(contentRunes)
	partNum := 0

	for i := 0; i < totalLength; i += chunkSize {
		end := i + chunkSize
		if end > totalLength {
			end = totalLength
		}

		// Ensure we don't break words - find the last space before the chunk end
		if end < totalLength {
			for j := end - 1; j > i; j-- {
				if contentRunes[j] == ' ' || contentRunes[j] == '\n' || contentRunes[j] == '\t' {
					end = j
					break
				}
			}
		}

		chunk := string(contentRunes[i:end])
		if strings.TrimSpace(chunk) != "" {
			jobs = append(jobs, Job{
				JobID:   jobID,
				PartNum: partNum,
				Content: chunk,
			})
			partNum++
		}
	}

	log.Printf("Split content into %d jobs", len(jobs))
	return jobs
}

// HTTP handler for receiving results from workers
func (mrs *MapReduceServer) handleResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var result JobResult
	decoder := json.NewDecoder(r.Body)
	if err := decoder.Decode(&result); err != nil {
		log.Printf("Error decoding result: %v", err)
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	mrs.mu.Lock()
	defer mrs.mu.Unlock()

	jobKey := fmt.Sprintf("%s-%d", result.JobID, result.PartNum)
	log.Printf("Received result from worker %s for job %s part %d: %d words, %d unique",
		result.WorkerID, result.JobID, result.PartNum, result.TotalWords, result.UniqueWords)

	// Remove from in-progress tracking
	delete(mrs.jobInProgress, jobKey)

	// Store the result
	mrs.completedResults[jobKey] = result
	mrs.totalProcessed++

	// Merge word counts into final result
	for word, count := range result.WordCount {
		mrs.finalWordCount[word] += count
	}

	log.Printf("Progress: %d/%d jobs completed", mrs.totalProcessed, mrs.totalJobs)

	// Check if all jobs are completed
	if mrs.totalProcessed >= mrs.totalJobs {
		mrs.printFinalResults()
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}

// Print final aggregated results
func (mrs *MapReduceServer) printFinalResults() {
	log.Printf("=== FINAL RESULTS ===")
	log.Printf("Total unique words: %d", len(mrs.finalWordCount))

	totalWords := 0
	for _, count := range mrs.finalWordCount {
		totalWords += count
	}
	log.Printf("Total word occurrences: %d", totalWords)

	// Print top 20 most frequent words
	type wordFreq struct {
		word  string
		count int
	}

	var wordFreqs []wordFreq
	for word, count := range mrs.finalWordCount {
		wordFreqs = append(wordFreqs, wordFreq{word, count})
	}

	// Simple bubble sort for top words (good enough for demonstration)
	for i := 0; i < len(wordFreqs)-1; i++ {
		for j := 0; j < len(wordFreqs)-i-1; j++ {
			if wordFreqs[j].count < wordFreqs[j+1].count {
				wordFreqs[j], wordFreqs[j+1] = wordFreqs[j+1], wordFreqs[j]
			}
		}
	}

	log.Printf("Top 20 most frequent words:")
	limit := 20
	if len(wordFreqs) < limit {
		limit = len(wordFreqs)
	}

	for i := 0; i < limit; i++ {
		log.Printf("%d. %s: %d", i+1, wordFreqs[i].word, wordFreqs[i].count)
	}
}

// Health check endpoint
func (mrs *MapReduceServer) handleHealth(w http.ResponseWriter, r *http.Request) {
	mrs.mu.RLock()
	defer mrs.mu.RUnlock()

	status := map[string]interface{}{
		"status":           "healthy",
		"total_jobs":       mrs.totalJobs,
		"completed_jobs":   mrs.totalProcessed,
		"pending_jobs":     len(mrs.pendingJobs),
		"jobs_in_progress": len(mrs.jobInProgress),
		"unique_words":     len(mrs.finalWordCount),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

// Status endpoint for monitoring
func (mrs *MapReduceServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	mrs.mu.RLock()
	defer mrs.mu.RUnlock()

	totalWords := 0
	for _, count := range mrs.finalWordCount {
		totalWords += count
	}

	status := map[string]interface{}{
		"total_jobs":         mrs.totalJobs,
		"completed_jobs":     mrs.totalProcessed,
		"pending_jobs":       len(mrs.pendingJobs),
		"jobs_in_progress":   len(mrs.jobInProgress),
		"unique_words":       len(mrs.finalWordCount),
		"total_word_count":   totalWords,
		"completion_percent": float64(mrs.totalProcessed) / float64(mrs.totalJobs) * 100,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(status)
}

func main() {
	// Configuration from environment variables
	httpPort := os.Getenv("HTTP_PORT")
	if httpPort == "" {
		httpPort = "8080"
	}

	rpcPort := os.Getenv("RPC_PORT")
	if rpcPort == "" {
		rpcPort = "1234"
	}

	fileURL := os.Getenv("FILE_URL")
	if fileURL == "" {
		fileURL = "https://s3.g.s4.mega.io/2cnjyiqqebttwjrnbitzpvy6uw4j7ujo4bxnu/jiahua-bucket/test.txt"
	}

	chunkSizeStr := os.Getenv("CHUNK_SIZE")
	chunkSize := 10000 // Default chunk size in characters
	if chunkSizeStr != "" {
		if size, err := strconv.Atoi(chunkSizeStr); err == nil {
			chunkSize = size
		}
	}

	log.Printf("Starting MapReduce Server...")
	log.Printf("HTTP Port: %s", httpPort)
	log.Printf("RPC Port: %s", rpcPort)
	log.Printf("File URL: %s", fileURL)
	log.Printf("Chunk Size: %d characters", chunkSize)

	// Initialize server state
	server := &MapReduceServer{
		completedResults: make(map[string]JobResult),
		finalWordCount:   make(map[string]int),
		jobInProgress:    make(map[string]time.Time),
		maxRetries:       3,
		retryTimeout:     5 * time.Minute,
	}

	// Download file from URL
	content, err := downloadFromURL(fileURL)
	if err != nil {
		log.Fatalf("Failed to download file: %v", err)
	}

	// Split content into jobs
	jobID := fmt.Sprintf("wordcount-%d", time.Now().Unix())
	server.jobs = splitContentIntoJobs(content, jobID, chunkSize)
	server.pendingJobs = make([]Job, len(server.jobs))
	copy(server.pendingJobs, server.jobs)
	server.totalJobs = len(server.jobs)

	log.Printf("Created %d jobs for processing", server.totalJobs)

	// Start RPC server for job distribution
	jobServer := &JobServer{server: server}
	rpc.Register(jobServer)

	rpcListener, err := net.Listen("tcp", ":"+rpcPort)
	if err != nil {
		log.Fatalf("Failed to start RPC server: %v", err)
	}

	go func() {
		log.Printf("RPC server listening on port %s", rpcPort)
		for {
			conn, err := rpcListener.Accept()
			if err != nil {
				log.Printf("RPC accept error: %v", err)
				continue
			}
			go rpc.ServeConn(conn)
		}
	}()

	// Start HTTP server for result collection and monitoring
	router := mux.NewRouter()
	router.HandleFunc("/results", server.handleResults).Methods("POST")
	router.HandleFunc("/health", server.handleHealth).Methods("GET")
	router.HandleFunc("/status", server.handleStatus).Methods("GET")

	log.Printf("HTTP server starting on port %s", httpPort)
	log.Printf("Ready to accept workers!")
	log.Fatal(http.ListenAndServe(":"+httpPort, router))
}
