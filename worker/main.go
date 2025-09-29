package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strings"
	"time"
)

type Job struct {
	JobID   string
	PartNum int
	Content string
}

type JobResult struct {
	JobID       string         `json:"job_id"`
	PartNum     int            `json:"part_num"`
	WorkerID    string         `json:"worker_id"`
	WordCount   map[string]int `json:"word_count"`
	TotalWords  int            `json:"total_words"`
	UniqueWords int            `json:"unique_words"`
	Status      string         `json:"status"`
}

type GetJobArgs struct {
	WorkerID string
}

type GetJobReply struct {
	Job    Job
	HasJob bool
}

func main() {
	// Get configuration from environment variables
	workerID := os.Getenv("WORKER_ID")
	if workerID == "" {
		workerID = fmt.Sprintf("worker-%d", time.Now().Unix())
	}

	rpcServer := os.Getenv("RPC_SERVER_ADDR")
	if rpcServer == "" {
		rpcServer = "localhost:1234"
	}

	reportURL := os.Getenv("REPORT_BACK_URL")
	if reportURL == "" {
		reportURL = "http://localhost:8080/results"
	}

	log.Printf("Starting word counter worker with ID: %s", workerID)
	log.Printf("RPC Server: %s", rpcServer)
	log.Printf("Report URL: %s", reportURL)

	client, err := rpc.Dial("tcp", rpcServer)
	if err != nil {
		log.Fatalf("Failed to connect to RPC server: %v", err)
	}
	defer client.Close()

	log.Println("Connected to RPC server, starting work loop...")

	for {
		// Get job
		var reply GetJobReply
		err := client.Call("JobServer.GetJob", GetJobArgs{WorkerID: workerID}, &reply)
		if err != nil {
			log.Printf("Error getting job from RPC server: %v", err)
			time.Sleep(5 * time.Second)
			continue
		}

		if !reply.HasJob {
			time.Sleep(5 * time.Second)
			continue
		}

		// Count words
		words := strings.Fields(strings.ToLower(reply.Job.Content))
		wordCount := make(map[string]int)
		for _, word := range words {
			wordCount[word]++
		}

		// Create result
		result := JobResult{
			JobID:       reply.Job.JobID,
			PartNum:     reply.Job.PartNum,
			WorkerID:    workerID,
			WordCount:   wordCount,
			TotalWords:  len(words),
			UniqueWords: len(wordCount),
			Status:      "completed",
		}

		// Report result
		jsonData, _ := json.Marshal(result)
		http.Post(reportURL, "application/json", bytes.NewBuffer(jsonData))

		log.Printf("Completed job %s part %d", result.JobID, result.PartNum)
	}
}
