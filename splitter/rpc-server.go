package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
)

// Core data structures
type FileSplitter struct {
	mu       sync.RWMutex
	files    map[string]*SplitFile
	tempDir  string
	jobQueue *JobQueue
}

type SplitFile struct {
	URL       string
	NumParts  int
	Filename  string
	FilePath  string
	FileSize  int64
	Parts     []FilePart
	CreatedAt int64
}

type FilePart struct {
	PartNumber int
	Start      int
	End        int
	Size       int
}

// Job management structures
type Job struct {
	JobID      string `json:"job_id"`
	URL        string `json:"url"`
	PartNum    int    `json:"part_num"`
	Content    string `json:"content"`
	TotalParts int    `json:"total_parts"`
}

type JobQueue struct {
	mu         sync.RWMutex
	jobs       chan Job
	activeJobs map[string]*ActiveJob
	results    map[string]*JobResults
}

type ActiveJob struct {
	JobID      string
	URL        string
	TotalParts int
	PartsLeft  int
	CreatedAt  time.Time
}

type JobResults struct {
	JobID       string             `json:"job_id"`
	URL         string             `json:"url"`
	TotalParts  int                `json:"total_parts"`
	Parts       map[int]*JobResult `json:"parts"`
	FinalResult *AggregatedResult  `json:"final_result,omitempty"`
	Status      string             `json:"status"` // "processing", "completed", "failed"
	CreatedAt   time.Time          `json:"created_at"`
	CompletedAt *time.Time         `json:"completed_at,omitempty"`
}

type JobResult struct {
	JobID          string         `json:"job_id"`
	PartNum        int            `json:"part_num"`
	WorkerID       string         `json:"worker_id"`
	WordCount      map[string]int `json:"word_count"`
	TotalWords     int            `json:"total_words"`
	UniqueWords    int            `json:"unique_words"`
	ProcessingTime int64          `json:"processing_time_ms"`
	Status         string         `json:"status"`
	Error          string         `json:"error,omitempty"`
}

type AggregatedResult struct {
	TotalWords     int            `json:"total_words"`
	UniqueWords    int            `json:"unique_words"`
	WordCount      map[string]int `json:"word_count"`
	ProcessingTime int64          `json:"total_processing_time_ms"`
	PartsProcessed int            `json:"parts_processed"`
}

// RPC request/response structures
type GetJobArgs struct {
	WorkerID string
}

type GetJobReply struct {
	Job    Job  `json:"job"`
	HasJob bool `json:"has_job"`
}

// HTTP request structures
type SplitRequest struct {
	URL      string `json:"url"`
	NumParts int    `json:"num_parts"`
}

type SplitResponse struct {
	JobID    string     `json:"job_id"`
	URL      string     `json:"url"`
	NumParts int        `json:"num_parts"`
	Parts    []FilePart `json:"parts"`
	FileSize int64      `json:"file_size"`
}

func NewFileSplitter() *FileSplitter {
	tempDir := "/tmp/splitter"
	os.MkdirAll(tempDir, 0755)

	jq := &JobQueue{
		jobs:       make(chan Job, 1000), // Buffer for jobs
		activeJobs: make(map[string]*ActiveJob),
		results:    make(map[string]*JobResults),
	}

	return &FileSplitter{
		files:    make(map[string]*SplitFile),
		tempDir:  tempDir,
		jobQueue: jq,
	}
}

// RPC Methods for JobQueue
func (jq *JobQueue) GetJob(args GetJobArgs, reply *GetJobReply) error {
	select {
	case job := <-jq.jobs:
		log.Printf("Dispatching job %s part %d to worker %s", job.JobID, job.PartNum, args.WorkerID)
		reply.Job = job
		reply.HasJob = true
		return nil
	default:
		reply.HasJob = false
		return nil
	}
}

func (fs *FileSplitter) downloadFile(fileURL string) (string, error) {
	parsedURL, err := url.Parse(fileURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	parts := strings.Split(parsedURL.Path, "/")
	filename := parts[len(parts)-1]
	if filename == "" {
		filename = "downloaded_file.txt"
	}

	filepath := fmt.Sprintf("%s/%s", fs.tempDir, filename)

	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: HTTP %d", resp.StatusCode)
	}

	file, err := os.Create(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save file: %w", err)
	}

	return filepath, nil
}

func (fs *FileSplitter) splitFileInMemory(filepath string, numParts int) (*SplitFile, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Read entire file content for splitting
	content, err := io.ReadAll(file)
	if err != nil {
		return nil, fmt.Errorf("failed to read file: %w", err)
	}

	text := string(content)
	chunkSize := len(text) / numParts

	splitPoints := make([]int, 0, numParts-1)

	for i := 1; i < numParts; i++ {
		targetPos := i * chunkSize
		pos := findWordBoundary(text, targetPos)
		if pos > 0 && pos < len(text) {
			splitPoints = append(splitPoints, pos)
		}
	}

	parts := make([]FilePart, 0, numParts)
	start := 0

	for i, end := range splitPoints {
		parts = append(parts, FilePart{
			PartNumber: i + 1,
			Start:      start,
			End:        end,
			Size:       end - start,
		})
		start = end
	}

	parts = append(parts, FilePart{
		PartNumber: len(splitPoints) + 1,
		Start:      start,
		End:        len(text),
		Size:       len(text) - start,
	})

	return &SplitFile{
		NumParts: numParts,
		Filename: info.Name(),
		FilePath: filepath,
		FileSize: fileSize,
		Parts:    parts,
	}, nil
}

func findWordBoundary(text string, targetPos int) int {
	if targetPos >= len(text) {
		return len(text)
	}

	for i := targetPos; i < len(text); i++ {
		if text[i] == ' ' || text[i] == '\n' || text[i] == '\t' {
			for i < len(text) && (text[i] == ' ' || text[i] == '\n' || text[i] == '\t') {
				i++
			}
			return i
		}
	}

	return len(text)
}

func (fs *FileSplitter) getFilePart(filepath string, part FilePart) (string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	_, err = file.Seek(int64(part.Start), 0)
	if err != nil {
		return "", fmt.Errorf("failed to seek to position: %w", err)
	}

	buffer := make([]byte, part.Size)
	n, err := file.Read(buffer)
	if err != nil && err != io.EOF {
		return "", fmt.Errorf("failed to read file part: %w", err)
	}

	return string(buffer[:n]), nil
}

// HTTP Handlers

func (fs *FileSplitter) handleSplit(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var req SplitRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	if req.URL == "" || req.NumParts < 1 {
		http.Error(w, "URL and num_parts (>0) are required", http.StatusBadRequest)
		return
	}

	jobID := fmt.Sprintf("job-%d", time.Now().Unix())

	fs.mu.Lock()
	defer fs.mu.Unlock()

	// Check if already processed
	if existing, exists := fs.files[jobID]; exists {
		response := SplitResponse{
			JobID:    jobID,
			URL:      existing.URL,
			NumParts: existing.NumParts,
			Parts:    existing.Parts,
			FileSize: existing.FileSize,
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(response)
		return
	}

	// Download file
	filepath, err := fs.downloadFile(req.URL)
	if err != nil {
		http.Error(w, fmt.Sprintf("Download failed: %v", err), http.StatusBadRequest)
		return
	}

	// Split file
	splitFile, err := fs.splitFileInMemory(filepath, req.NumParts)
	if err != nil {
		os.Remove(filepath)
		http.Error(w, fmt.Sprintf("Split failed: %v", err), http.StatusInternalServerError)
		return
	}

	splitFile.URL = req.URL
	fs.files[jobID] = splitFile

	// Create jobs for workers
	jq := fs.jobQueue
	jq.mu.Lock()

	// Initialize job tracking
	jq.activeJobs[jobID] = &ActiveJob{
		JobID:      jobID,
		URL:        req.URL,
		TotalParts: req.NumParts,
		PartsLeft:  req.NumParts,
		CreatedAt:  time.Now(),
	}

	jq.results[jobID] = &JobResults{
		JobID:      jobID,
		URL:        req.URL,
		TotalParts: req.NumParts,
		Parts:      make(map[int]*JobResult),
		Status:     "processing",
		CreatedAt:  time.Now(),
	}

	// Queue individual part jobs
	for _, part := range splitFile.Parts {
		content, err := fs.getFilePart(filepath, part)
		if err != nil {
			log.Printf("Error reading part %d: %v", part.PartNumber, err)
			continue
		}

		job := Job{
			JobID:      jobID,
			URL:        req.URL,
			PartNum:    part.PartNumber,
			Content:    content,
			TotalParts: req.NumParts,
		}

		select {
		case jq.jobs <- job:
			log.Printf("Queued job %s part %d", jobID, part.PartNumber)
		default:
			log.Printf("Job queue full, skipping part %d", part.PartNumber)
		}
	}

	jq.mu.Unlock()

	response := SplitResponse{
		JobID:    jobID,
		URL:      req.URL,
		NumParts: req.NumParts,
		Parts:    splitFile.Parts,
		FileSize: splitFile.FileSize,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (fs *FileSplitter) handleResults(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	var result JobResult
	if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
		http.Error(w, "Invalid JSON", http.StatusBadRequest)
		return
	}

	log.Printf("Received result for job %s part %d from worker %s",
		result.JobID, result.PartNum, result.WorkerID)

	jq := fs.jobQueue
	jq.mu.Lock()
	defer jq.mu.Unlock()

	// Check if job exists
	jobResults, exists := jq.results[result.JobID]
	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	// Store the result
	jobResults.Parts[result.PartNum] = &result

	// Check if all parts are completed
	completedParts := 0
	for i := 1; i <= jobResults.TotalParts; i++ {
		if part, exists := jobResults.Parts[i]; exists && part.Status == "completed" {
			completedParts++
		}
	}

	log.Printf("Job %s: %d/%d parts completed", result.JobID, completedParts, jobResults.TotalParts)

	// If all parts are done, aggregate results
	if completedParts == jobResults.TotalParts {
		fs.aggregateResults(jobResults)
	}

	w.WriteHeader(http.StatusOK)
	json.NewEncoder(w).Encode(map[string]string{"status": "received"})
}

func (fs *FileSplitter) aggregateResults(jobResults *JobResults) {
	log.Printf("Aggregating results for job %s", jobResults.JobID)

	aggregated := &AggregatedResult{
		WordCount:      make(map[string]int),
		PartsProcessed: len(jobResults.Parts),
	}

	for _, part := range jobResults.Parts {
		if part.Status != "completed" {
			continue
		}

		aggregated.TotalWords += part.TotalWords
		aggregated.ProcessingTime += part.ProcessingTime

		// Merge word counts
		for word, count := range part.WordCount {
			aggregated.WordCount[word] += count
		}
	}

	aggregated.UniqueWords = len(aggregated.WordCount)

	jobResults.FinalResult = aggregated
	jobResults.Status = "completed"
	now := time.Now()
	jobResults.CompletedAt = &now

	log.Printf("Job %s completed: %d total words, %d unique words",
		jobResults.JobID, aggregated.TotalWords, aggregated.UniqueWords)
}

func (fs *FileSplitter) handleJobStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	jq := fs.jobQueue
	jq.mu.RLock()
	jobResults, exists := jq.results[jobID]
	jq.mu.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(jobResults)
}

func (fs *FileSplitter) handleGetPart(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]
	partNumStr := vars["partNum"]

	partNum, err := strconv.Atoi(partNumStr)
	if err != nil {
		http.Error(w, "Invalid part number", http.StatusBadRequest)
		return
	}

	fs.mu.RLock()
	splitFile, exists := fs.files[jobID]
	fs.mu.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	if partNum < 1 || partNum > len(splitFile.Parts) {
		http.Error(w, "Part number out of range", http.StatusBadRequest)
		return
	}

	part := splitFile.Parts[partNum-1]
	content, err := fs.getFilePart(splitFile.FilePath, part)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get part: %v", err), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/plain")
	w.Header().Set("Content-Length", strconv.Itoa(len(content)))
	w.Write([]byte(content))
}

func (fs *FileSplitter) handleHealth(w http.ResponseWriter, r *http.Request) {
	jq := fs.jobQueue
	jq.mu.RLock()
	activeJobs := len(jq.activeJobs)
	queuedJobs := len(jq.jobs)
	completedJobs := 0

	for _, result := range jq.results {
		if result.Status == "completed" {
			completedJobs++
		}
	}
	jq.mu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "healthy",
		"active_jobs":    activeJobs,
		"queued_jobs":    queuedJobs,
		"completed_jobs": completedJobs,
	})
}

func startRPCServer(jq *JobQueue) {
	rpc.Register(jq)
	rpc.HandleHTTP()

	rpcPort := os.Getenv("RPC_PORT")
	if rpcPort == "" {
		rpcPort = "1234"
	}

	listener, err := net.Listen("tcp", ":"+rpcPort)
	if err != nil {
		log.Fatal("Failed to start RPC server:", err)
	}

	log.Printf("RPC server listening on port %s", rpcPort)
	go http.Serve(listener, nil)
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fs := NewFileSplitter()

	// Start RPC server
	startRPCServer(fs.jobQueue)

	r := mux.NewRouter()

	// API routes
	r.HandleFunc("/split", fs.handleSplit).Methods("POST")
	r.HandleFunc("/results", fs.handleResults).Methods("POST")
	r.HandleFunc("/job/{jobId}/status", fs.handleJobStatus).Methods("GET")
	r.HandleFunc("/job/{jobId}/part/{partNum}", fs.handleGetPart).Methods("GET")
	r.HandleFunc("/health", fs.handleHealth).Methods("GET")

	// Documentation
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`
<!DOCTYPE html>
<html>
<head><title>File Splitter RPC Server</title></head>
<body>
<h1>File Splitter RPC Server</h1>
<h2>HTTP Endpoints:</h2>
<ul>
<li><strong>POST /split</strong> - Split a file and queue jobs for workers</li>
<li><strong>POST /results</strong> - Workers report job completion results</li>
<li><strong>GET /job/{jobId}/status</strong> - Get job status and aggregated results</li>
<li><strong>GET /job/{jobId}/part/{partNum}</strong> - Get a specific part (for testing)</li>
<li><strong>GET /health</strong> - Server health and queue status</li>
</ul>
<h2>RPC Service:</h2>
<ul>
<li><strong>JobQueue.GetJob</strong> - Workers call this to get work (port 1234)</li>
</ul>
</body>
</html>
		`))
	})

	log.Printf("HTTP server starting on port %s", port)
	log.Printf("RPC server available for worker connections")
	log.Fatal(http.ListenAndServe(":"+port, r))
}
