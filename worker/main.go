package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

// Job represents a word counting job received from RPC server
type Job struct {
	JobID      string `json:"job_id"`
	URL        string `json:"url"`
	PartNum    int    `json:"part_num"`
	Content    string `json:"content"`
	TotalParts int    `json:"total_parts"`
}

// JobResult represents the result to be reported back
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

// RPC Args and Reply types
type GetJobArgs struct {
	WorkerID string
}

type GetJobReply struct {
	Job    Job
	HasJob bool
}

// Worker configuration
type WorkerConfig struct {
	WorkerID      string
	RPCServerAddr string
	ReportBackURL string
	PollInterval  time.Duration
}

func countWordOccurrences(text string) map[string]int {
	// Convert to lowercase and split into words
	words := strings.Fields(strings.ToLower(text))
	wordCount := make(map[string]int)

	// Count each word
	for _, word := range words {
		// Remove punctuation from the end of words
		word = strings.Trim(word, ".,!?;:")
		if word != "" {
			wordCount[word]++
		}
	}

	return wordCount
}

func processJob(job Job, workerID string) JobResult {
	startTime := time.Now()

	result := JobResult{
		JobID:    job.JobID,
		PartNum:  job.PartNum,
		WorkerID: workerID,
		Status:   "completed",
	}

	// Count word occurrences
	wordCounts := countWordOccurrences(job.Content)

	totalWords := 0
	for _, count := range wordCounts {
		totalWords += count
	}

	result.WordCount = wordCounts
	result.TotalWords = totalWords
	result.UniqueWords = len(wordCounts)
	result.ProcessingTime = time.Since(startTime).Milliseconds()

	return result
}

func reportResult(result JobResult, reportURL string) error {
	jsonData, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("failed to marshal result: %v", err)
	}

	resp, err := http.Post(reportURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return fmt.Errorf("failed to post result: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("server responded with status %d: %s", resp.StatusCode, string(body))
	}

	return nil
}

func getJobFromRPC(client *rpc.Client, workerID string) (Job, bool, error) {
	args := GetJobArgs{WorkerID: workerID}
	var reply GetJobReply

	err := client.Call("JobQueue.GetJob", args, &reply)
	if err != nil {
		return Job{}, false, err
	}

	return reply.Job, reply.HasJob, nil
}

func loadConfig() WorkerConfig {
	config := WorkerConfig{
		WorkerID:      getEnv("WORKER_ID", "worker-"+strconv.FormatInt(time.Now().Unix(), 10)),
		RPCServerAddr: getEnv("RPC_SERVER_ADDR", "localhost:1234"),
		ReportBackURL: getEnv("REPORT_BACK_URL", "http://localhost:8080/results"),
		PollInterval:  parseDuration(getEnv("POLL_INTERVAL", "5s")),
	}
	return config
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func parseDuration(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 5 * time.Second
	}
	return d
}

func main() {
	config := loadConfig()

	log.Printf("Starting word counter worker with ID: %s", config.WorkerID)
	log.Printf("RPC Server: %s", config.RPCServerAddr)
	log.Printf("Report URL: %s", config.ReportBackURL)

	// Connect to RPC server
	client, err := rpc.Dial("tcp", config.RPCServerAddr)
	if err != nil {
		log.Fatalf("Failed to connect to RPC server: %v", err)
	}
	defer client.Close()

	log.Println("Connected to RPC server, starting work loop...")

	// Main work loop
	for {
		// Get job from RPC server
		job, hasJob, err := getJobFromRPC(client, config.WorkerID)
		if err != nil {
			log.Printf("Error getting job from RPC server: %v", err)
			time.Sleep(config.PollInterval)
			continue
		}

		if !hasJob {
			// No job available, wait and try again
			time.Sleep(config.PollInterval)
			continue
		}

		log.Printf("Received job %s, part %d/%d", job.JobID, job.PartNum, job.TotalParts)

		// Process the job
		result := processJob(job, config.WorkerID)

		// Report result back
		if err := reportResult(result, config.ReportBackURL); err != nil {
			log.Printf("Error reporting result: %v", err)
			// Mark as failed and report the error
			result.Status = "failed"
			result.Error = err.Error()
			if err := reportResult(result, config.ReportBackURL); err != nil {
				log.Printf("Failed to report error result: %v", err)
			}
		} else {
			log.Printf("Successfully completed and reported job %s part %d", job.JobID, job.PartNum)
		}
	}
}
