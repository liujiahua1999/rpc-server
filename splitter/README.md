# File Splitter Service

A high-performance containerized service that downloads text files from public URLs (including S3) and splits them into parts without cutting words. Built with Go for maximum speed and efficiency.

## Features

- **Fast file splitting**: Uses memory mapping for O(1) file access
- **Word boundary preservation**: Never cuts words in half
- **HTTP API**: RESTful endpoints for easy integration
- **S3 compatible**: Downloads from any public HTTP/HTTPS URL
- **Containerized**: Ready for Docker deployment
- **Memory efficient**: Processes large files without loading entirely into memory

## API Endpoints

### POST /split
Split a file from a public URL into parts.

**Request:**
```json
{
  "url": "https://example.com/file.txt",
  "num_parts": 4
}
```

**Response:**
```json
{
  "job_id": "12345678",
  "url": "https://example.com/file.txt",
  "num_parts": 4,
  "parts": [
    {
      "PartNumber": 1,
      "Start": 0,
      "End": 25000,
      "Size": 25000
    }
  ],
  "file_size": 100000
}
```

### GET /job/{jobId}/part/{partNum}
Retrieve a specific part of a split file.

**Response:** Plain text content of the requested part.

### GET /job/{jobId}/status
Get information about a split job.

**Response:** Same as POST /split response.

### GET /health
Health check endpoint.

## Usage Examples

### 1. Split a file
```bash
curl -X POST http://localhost:8080/split \
  -H "Content-Type: application/json" \
  -d '{"url": "https://www.gutenberg.org/files/74/74-0.txt", "num_parts": 4}'
```

### 2. Get a specific part
```bash
curl http://localhost:8080/job/68747470/part/1
```

### 3. Check job status
```bash
curl http://localhost:8080/job/68747470/status
```

## Docker Deployment

### Build and run with Docker Compose
```bash
docker-compose up --build
```

### Build and run manually
```bash
# Build
docker build -t file-splitter .

# Run
docker run -p 8080:8080 file-splitter
```

## Local Development

### Prerequisites
- Go 1.23+

### Run locally
```bash
go mod tidy
go run server.go
```

The server will start on port 8080 (or the port specified in the `PORT` environment variable).

## Performance Features

1. **Memory Mapping**: Uses `mmap` for O(1) file access
2. **Efficient Word Boundary Detection**: Finds word boundaries without parsing entire content
3. **Minimal Memory Allocation**: Avoids copying large file contents
4. **Concurrent Safe**: Supports multiple simultaneous requests

## File Size Limits

The service can handle files up to the available system memory. For very large files, consider:
- Increasing container memory limits
- Using persistent volumes for temporary file storage

## Environment Variables

- `PORT`: Server port (default: 8080)

## Error Handling

The service returns appropriate HTTP status codes:
- `200`: Success
- `400`: Bad request (invalid URL, invalid num_parts)
- `404`: Job not found
- `500`: Internal server error

## License

This project is open source and available under the MIT License.