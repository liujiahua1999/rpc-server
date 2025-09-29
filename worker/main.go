package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"
	"net/rpc"
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
	workerID := "worker-1"
	rpcAddr := "localhost:1234"
	reportURL := "http://localhost:8080/results"

	client, err := rpc.Dial("tcp", rpcAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	for {
		// Get job
		var reply GetJobReply
		err := client.Call("JobQueue.GetJob", GetJobArgs{WorkerID: workerID}, &reply)
		if err != nil {
			log.Println(err)
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
