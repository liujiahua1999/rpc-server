# Word Counter Worker Service

A containerized Go service that processes text files by counting word occurrences. This worker connects to an RPC server to receive jobs, processes file parts, and reports results back via HTTP.

## Features

- **RPC Integration**: Connects to RPC server to receive word counting jobs
- **HTTP Reporting**: Reports results back to server via REST API
- **Containerized**: Docker-ready with multi-stage builds
- **Scalable**: Stateless design allows horizontal scaling
- **Configurable**: Environment variable based configuration
- **Resilient**: Automatic retry on failures and connection errors

## Architecture Overview

```
[RPC Server] ←--RPC--> [Worker] --HTTP--> [Results Endpoint]
     │                    │
     ├─ Job Queue         ├─ Word Counter
     └─ Results Handler   └─ HTTP Client
```

## Quick Start

### Using Docker Compose

1. **Build and run the service:**
   ```bash
   docker-compose up --build
   ```

2. **Scale workers:**
   ```bash
   docker-compose up --scale word-counter-worker-1=3
   ```

### Using Docker

1. **Build the image:**
   ```bash
   docker build -t word-counter-worker .
   ```

2. **Run a worker:**
   ```bash
   docker run -e WORKER_ID=worker-1 \
              -e RPC_SERVER_ADDR=your-rpc-server:1234 \
              -e REPORT_BACK_URL=http://your-server:8080/results \
              word-counter-worker
   ```

## Configuration

Configure the worker using environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `WORKER_ID` | `worker-{timestamp}` | Unique identifier for this worker |
| `RPC_SERVER_ADDR` | `localhost:1234` | RPC server address (host:port) |
| `REPORT_BACK_URL` | `http://localhost:8080/results` | HTTP endpoint for reporting results |
| `POLL_INTERVAL` | `5s` | How often to poll RPC server for jobs |

### Example Configuration

```bash
export WORKER_ID="worker-prod-1"
export RPC_SERVER_ADDR="rpc.example.com:1234"
export REPORT_BACK_URL="https://api.example.com/word-count/results"
export POLL_INTERVAL="3s"
```

## How It Works

### 1. Job Acquisition
- Worker connects to RPC server on startup
- Polls the server every `POLL_INTERVAL` for available jobs
- Receives job with file content and metadata

### 2. Word Processing
- Processes text content using efficient word counting algorithm
- Converts text to lowercase for consistent counting
- Removes punctuation from word endings
- Counts total words, unique words, and word frequencies

### 3. Result Reporting
- Packages results with metadata (processing time, worker ID, etc.)
- Posts results to HTTP endpoint as JSON
- Handles errors gracefully and reports failures

### Word Counting Algorithm

```go
// Text preprocessing
text = strings.ToLower(text)
words = strings.Fields(text)

// Word cleaning and counting
for each word in words {
    cleanWord = strings.Trim(word, ".,!?;:")
    if cleanWord != "" {
        wordCount[cleanWord]++
    }
}
```

## API Integration

### RPC Protocol

**Method**: `JobQueue.GetJob`

**Request**:
```go
type GetJobArgs struct {
    WorkerID string
}
```

**Response**:
```go
type GetJobReply struct {
    Job    Job
    HasJob bool
}

type Job struct {
    JobID      string `json:"job_id"`
    URL        string `json:"url"`
    PartNum    int    `json:"part_num"`
    Content    string `json:"content"`
    TotalParts int    `json:"total_parts"`
}
```

### HTTP Reporting

**Endpoint**: `POST {REPORT_BACK_URL}`

**Payload**:
```json
{
    "job_id": "job-12345",
    "part_num": 1,
    "worker_id": "worker-1",
    "word_count": {
        "the": 145,
        "and": 87,
        "to": 76
    },
    "total_words": 1247,
    "unique_words": 432,
    "processing_time_ms": 23,
    "status": "completed"
}
```

## Development

### Prerequisites
- Go 1.21 or later
- Docker (for containerization)

### Local Development

1. **Clone and build:**
   ```bash
   git clone <repository>
   cd word-counter/worker
   go mod tidy
   ```

2. **Run locally:**
   ```bash
   go run main.go
   ```

### Testing

1. **Unit tests:**
   ```bash
   go test -v
   ```

2. **Integration testing:**
   ```bash
   # Start mock RPC server (implement mock server first)
   docker-compose -f docker-compose.test.yml up
   ```

## Deployment

### Production Deployment

1. **Build production image:**
   ```bash
   docker build -t word-counter-worker:latest .
   ```

2. **Deploy with Kubernetes:**
   ```yaml
   apiVersion: apps/v1
   kind: Deployment
   metadata:
     name: word-counter-workers
   spec:
     replicas: 5
     selector:
       matchLabels:
         app: word-counter-worker
     template:
       metadata:
         labels:
           app: word-counter-worker
       spec:
         containers:
         - name: worker
           image: word-counter-worker:latest
           env:
           - name: RPC_SERVER_ADDR
             value: "rpc-service:1234"
           - name: REPORT_BACK_URL
             value: "http://api-service:8080/results"
           - name: WORKER_ID
             valueFrom:
               fieldRef:
                 fieldPath: metadata.name
   ```

### Scaling Considerations

- **Horizontal Scaling**: Add more worker instances
- **Resource Limits**: Set appropriate CPU/memory limits
- **Network**: Ensure RPC and HTTP endpoints are accessible
- **Monitoring**: Track worker health and job processing rates

## Monitoring

### Health Checks

The worker logs important events:
- Startup configuration
- RPC connection status
- Job processing events
- Error conditions

### Metrics to Monitor

- Worker uptime and restarts
- Jobs processed per minute
- Processing time per job
- Error rates
- RPC connection health

### Log Format

```
2024/01/01 12:00:00 Starting word counter worker with ID: worker-1
2024/01/01 12:00:01 Connected to RPC server, starting work loop...
2024/01/01 12:00:05 Received job job-123, part 1/4
2024/01/01 12:00:05 Successfully completed and reported job job-123 part 1
```

## Error Handling

### Connection Failures
- RPC connection errors: Continue polling with backoff
- HTTP reporting errors: Log error and continue with next job

### Processing Failures
- Invalid job content: Report error status to server
- Memory issues: Graceful degradation and error reporting

### Recovery Strategies
- Automatic reconnection to RPC server
- Retry failed HTTP reports
- Continue processing after transient errors

## Security

### Network Security
- Use TLS for production RPC connections
- Validate SSL certificates for HTTPS endpoints
- Implement proper firewall rules

### Data Handling
- No persistent storage of job content
- Secure logging (no sensitive data in logs)
- Memory cleanup after job completion

## Troubleshooting

### Common Issues

1. **Cannot connect to RPC server**
   ```bash
   # Check network connectivity
   telnet rpc-server 1234
   
   # Verify environment variables
   echo $RPC_SERVER_ADDR
   ```

2. **HTTP reporting failures**
   ```bash
   # Test endpoint manually
   curl -X POST -H "Content-Type: application/json" \
        -d '{"test": "data"}' $REPORT_BACK_URL
   ```

3. **Worker not receiving jobs**
   - Check RPC server has jobs in queue
   - Verify worker ID is unique
   - Check server logs for worker connections

### Debug Mode

Enable verbose logging:
```bash
export DEBUG=true
go run main.go
```

## Performance

### Benchmarks

Typical performance on standard hardware:
- **Small files** (< 1MB): 5-15ms processing time
- **Medium files** (1-10MB): 50-200ms processing time
- **Large files** (10-100MB): 500-2000ms processing time

### Optimization Tips

- Scale workers based on job queue depth
- Monitor memory usage for very large text files
- Use SSD storage for temporary files if needed

## License

This project is available under the MIT License.

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## Support

For issues and questions:
- Check the troubleshooting section
- Review logs for error messages
- Create an issue with relevant logs and configuration