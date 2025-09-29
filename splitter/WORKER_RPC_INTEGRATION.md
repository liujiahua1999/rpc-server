# Word Counter Worker - RPC Integration Documentation

## Overview

This document describes the RPC communication protocol and HTTP reporting mechanism used by the Word Counter Worker. This information is intended for the RPC server development team to understand how to integrate with the worker service.

## Architecture

```
┌─────────────────┐    RPC Calls     ┌──────────────────┐
│   RPC Server    │ ←────────────── │  Worker Instance │
│                 │                  │                  │
│  ┌─────────────┐│   Job Dispatch   │  ┌──────────────┐│
│  │ Job Queue   ││ ────────────────→│  │ Word Counter ││
│  └─────────────┘│                  │  └──────────────┘│
│                 │                  │                  │
│  ┌─────────────┐│  HTTP Reports    │  ┌──────────────┐│
│  │HTTP Endpoint││ ←────────────── │  │HTTP Client   ││
│  └─────────────┘│                  │  └──────────────┘│
└─────────────────┘                  └──────────────────┘
```

## RPC Protocol Specification

### Connection Details
- **Protocol**: TCP-based Go RPC
- **Default Port**: 1234
- **Service Name**: `JobQueue`

### RPC Methods

#### 1. GetJob Method

**Purpose**: Workers call this method to request work from the job queue.

**Method Signature**:
```go
func (jq *JobQueue) GetJob(args GetJobArgs, reply *GetJobReply) error
```

**Request Structure** (`GetJobArgs`):
```go
type GetJobArgs struct {
    WorkerID string  // Unique identifier for the worker instance
}
```

**Response Structure** (`GetJobReply`):
```go
type GetJobReply struct {
    Job    Job   // The job details (only valid if HasJob is true)
    HasJob bool  // Whether a job is available
}

type Job struct {
    JobID      string `json:"job_id"`      // Unique job identifier
    URL        string `json:"url"`         // Original file URL (for reference)
    PartNum    int    `json:"part_num"`    // Part number (1-based)
    Content    string `json:"content"`     // Text content to process
    TotalParts int    `json:"total_parts"` // Total number of parts in this job
}
```

**Behavior**:
- If no jobs are available, return `HasJob: false`
- If a job is available, populate `Job` and return `HasJob: true`
- Each job represents a portion of a split file that needs word counting
- The same job should not be given to multiple workers (implement job locking)

### Worker Polling Behavior
- Workers poll every 5 seconds by default (configurable via `POLL_INTERVAL`)
- Workers will retry on RPC errors with exponential backoff
- Workers maintain persistent connections to the RPC server

## HTTP Reporting Protocol

### Report Back Endpoint

**Purpose**: Workers use this endpoint to report job completion results.

**Method**: `POST`
**Content-Type**: `application/json`
**Default URL**: `http://rpc-server:8080/results`

### Request Payload

```go
type JobResult struct {
    JobID           string         `json:"job_id"`              // Job identifier (matches Job.JobID)
    PartNum         int            `json:"part_num"`            // Part number that was processed
    WorkerID        string         `json:"worker_id"`           // Worker that processed this job
    WordCount       map[string]int `json:"word_count"`          // Word frequency map
    TotalWords      int            `json:"total_words"`         // Total word count in this part
    UniqueWords     int            `json:"unique_words"`        // Number of unique words
    ProcessingTime  int64          `json:"processing_time_ms"`  // Processing time in milliseconds
    Status          string         `json:"status"`              // "completed" or "failed"
    Error           string         `json:"error,omitempty"`     // Error message if status is "failed"
}
```

### Example JSON Payload

```json
{
    "job_id": "job-12345",
    "part_num": 2,
    "worker_id": "worker-1",
    "word_count": {
        "the": 145,
        "and": 87,
        "to": 76,
        "of": 65,
        "a": 54
    },
    "total_words": 1247,
    "unique_words": 432,
    "processing_time_ms": 23,
    "status": "completed"
}
```

### Expected HTTP Responses

- **200 OK**: Result accepted successfully
- **400 Bad Request**: Invalid JSON or missing required fields
- **404 Not Found**: Job ID not found
- **500 Internal Server Error**: Server error processing result

## Word Counting Algorithm

The worker implements the following word counting logic:

1. **Text Preprocessing**:
   - Convert text to lowercase
   - Split on whitespace using `strings.Fields()`

2. **Word Cleaning**:
   - Remove punctuation from word endings: `.,!?;:`
   - Skip empty strings after cleaning

3. **Counting**:
   - Maintain a frequency map of all words
   - Count total words and unique words

## Error Handling

### RPC Connection Errors
- Workers will log errors and retry connection
- Failed RPC calls result in continued polling
- No automatic worker shutdown on RPC errors

### HTTP Reporting Errors
- Workers will attempt to report errors back to the server
- If error reporting fails, it's logged locally
- Workers continue processing new jobs after errors

## Configuration

Workers are configured via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_ID` | `worker-{timestamp}` | Unique worker identifier |
| `RPC_SERVER_ADDR` | `localhost:1234` | RPC server address |
| `REPORT_BACK_URL` | `http://localhost:8080/results` | HTTP reporting endpoint |
| `POLL_INTERVAL` | `5s` | How often to poll for jobs |

## Deployment Considerations

### Scaling Workers
- Workers are stateless and can be scaled horizontally
- Each worker should have a unique `WORKER_ID`
- Use container orchestration for automatic scaling

### Network Configuration
- Ensure RPC port (1234) is accessible to workers
- Ensure HTTP report endpoint is accessible from workers
- Consider using service discovery for dynamic addressing

### Resource Management
- Workers have minimal memory footprint
- CPU usage scales with text size and word diversity
- No persistent storage required

## Integration Recommendations for RPC Server

### 1. Job Queue Management
```go
// Implement thread-safe job queue
type JobQueue struct {
    jobs    chan Job
    results chan JobResult
    mu      sync.RWMutex
    activeJobs map[string]bool
}

func (jq *JobQueue) GetJob(args GetJobArgs, reply *GetJobReply) error {
    select {
    case job := <-jq.jobs:
        jq.mu.Lock()
        jq.activeJobs[job.JobID] = true
        jq.mu.Unlock()
        
        reply.Job = job
        reply.HasJob = true
        return nil
    default:
        reply.HasJob = false
        return nil
    }
}
```

### 2. HTTP Results Handler
```go
func handleResults(w http.ResponseWriter, r *http.Request) {
    var result JobResult
    if err := json.NewDecoder(r.Body).Decode(&result); err != nil {
        http.Error(w, "Invalid JSON", http.StatusBadRequest)
        return
    }
    
    // Process result
    if result.Status == "completed" {
        // Handle successful completion
        log.Printf("Job %s part %d completed by %s", 
            result.JobID, result.PartNum, result.WorkerID)
    } else {
        // Handle error
        log.Printf("Job %s part %d failed: %s", 
            result.JobID, result.PartNum, result.Error)
    }
    
    w.WriteHeader(http.StatusOK)
}
```

### 3. Job Distribution Strategy
- Implement fair job distribution among workers
- Consider worker capacity and processing speed
- Handle worker failures gracefully (job timeout/requeue)

### 4. Result Aggregation
- Collect results from all parts of a job
- Merge word counts across parts
- Notify clients when complete jobs are finished

## Monitoring and Observability

### Metrics to Collect
- Active worker count
- Jobs processed per minute
- Average processing time per job
- Error rates by worker
- Queue depth

### Logging
Workers log the following events:
- Startup configuration
- RPC connection status
- Job received/completed
- Errors in processing or reporting

## Testing the Integration

### Mock RPC Server
A simple test server can be created:

```go
// Test server that provides dummy jobs
func main() {
    rpc.Register(&MockJobQueue{})
    rpc.HandleHTTP()
    
    http.HandleFunc("/results", handleTestResults)
    
    listener, _ := net.Listen("tcp", ":1234")
    go http.Serve(listener, nil)
    
    http.ListenAndServe(":8080", nil)
}
```

### Validation Steps
1. Start RPC server with test jobs
2. Deploy worker containers
3. Verify workers connect and poll for jobs
4. Confirm results are reported back correctly
5. Test error scenarios (network failures, invalid jobs)

## Security Considerations

### Network Security
- Use TLS for production RPC connections
- Implement authentication for worker registration
- Validate worker IDs and rate limit requests

### Data Protection
- Ensure sensitive content is not logged
- Consider encrypting job content if required
- Implement proper access controls

This documentation should provide your RPC server team with all the information needed to implement a compatible server that can effectively manage and coordinate with the word counter workers.