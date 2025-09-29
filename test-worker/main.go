package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"strings"
	"time"
)

// RPC structures (must match server)
type GetJobArgs struct {
	WorkerID string
}

type GetJobReply struct {
	Job    Job  `json:"job"`
	HasJob bool `json:"has_job"`
}

type Job struct {
	JobID      string `json:"job_id"`
	URL        string `json:"url"`
	PartNum    int    `json:"part_num"`
	Content    string `json:"content"`
	TotalParts int    `json:"total_parts"`
}

type JobResult struct {
	JobID           string         `json:"job_id"`
	PartNum         int            `json:"part_num"`
	WorkerID        string         `json:"worker_id"`
	WordCount       map[string]int `json:"word_count"`
	TotalWords      int            `json:"total_words"`
	UniqueWords     int            `json:"unique_words"`
	ProcessingTime  int64          `json:"processing_time_ms"`
	Status          string         `json:"status"`
	Error           string         `json:"error,omitempty"`
}

func countWords(text string) (map[string]int, int, int) {
	wordCount := make(map[string]int)
	
	// Convert to lowercase and split by whitespace
	words := strings.Fields(strings.ToLower(text))
	
	for _, word := range words {
		// Clean punctuation from end
		word = strings.TrimRight(word, ".,!?;:")
		if word != "" {
			wordCount[word]++
		}
	}
	
	totalWords := len(words)
	uniqueWords := len(wordCount)
	
	return wordCount, totalWords, uniqueWords
}

func processJob(job Job, workerID string) JobResult {
	startTime := time.Now()
	
	log.Printf("Processing job %s part %d (%d chars)", job.JobID, job.PartNum, len(job.Content))
	
	wordCount, totalWords, uniqueWords := countWords(job.Content)
	
	processingTime := time.Since(startTime).Milliseconds()
	
	return JobResult{
		JobID:           job.JobID,
		PartNum:         job.PartNum,
		WorkerID:        workerID,
		WordCount:       wordCount,
		TotalWords:      totalWords,
		UniqueWords:     uniqueWords,
		ProcessingTime:  processingTime,
		Status:          "completed",
	}
}

func reportResult(result JobResult, reportURL string) error {
	jsonData, err := json.Marshal(result)
	if err != nil {
		return err
	}
	
	resp, err := http.Post(reportURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	
	log.Printf("Successfully reported result for job %s part %d", result.JobID, result.PartNum)
	return nil
}

func main() {
	workerID := "test-worker-1"
	rpcAddr := "localhost:1235"
	reportURL := "http://localhost:8082/results"
	
	log.Printf("Test worker %s starting", workerID)
	log.Printf("RPC server: %s", rpcAddr)
	log.Printf("Report URL: %s", reportURL)
	
	// Connect to RPC server
	client, err := rpc.DialHTTP("tcp", rpcAddr)
	if err != nil {
		log.Fatal("Failed to connect to RPC server:", err)
	}
	defer client.Close()
	
	// Work loop
	for {
		// Get job from server
		args := GetJobArgs{WorkerID: workerID}
		var reply GetJobReply
		
		err := client.Call("JobQueue.GetJob", args, &reply)
		if err != nil {
			log.Printf("RPC error: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}
		
		if !reply.HasJob {
			log.Printf("No jobs available, waiting...")
			time.Sleep(5 * time.Second)
			continue
		}
		
		// Process the job
		job := reply.Job
		result := processJob(job, workerID)
		
		// Report result back
		if err := reportResult(result, reportURL); err != nil {
			log.Printf("Failed to report result: %v", err)
			// Mark as failed and try to report the error
			result.Status = "failed"
			result.Error = err.Error()
			reportResult(result, reportURL)
		}
		
		// Small delay before getting next job
		time.Sleep(1 * time.Second)
	}
}