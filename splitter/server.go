package main

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"unicode"
	"unsafe"

	"github.com/gorilla/mux"
)

type FileSplitter struct {
	mu      sync.RWMutex
	files   map[string]*SplitFile
	tempDir string
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
	return &FileSplitter{
		files:   make(map[string]*SplitFile),
		tempDir: tempDir,
	}
}

func (fs *FileSplitter) downloadFile(fileURL string) (string, error) {
	// Parse URL to get filename
	parsedURL, err := url.Parse(fileURL)
	if err != nil {
		return "", fmt.Errorf("invalid URL: %w", err)
	}

	// Extract filename from URL path
	parts := strings.Split(parsedURL.Path, "/")
	filename := parts[len(parts)-1]
	if filename == "" {
		filename = "downloaded_file.txt"
	}

	filepath := fmt.Sprintf("%s/%s", fs.tempDir, filename)

	// Download file
	resp, err := http.Get(fileURL)
	if err != nil {
		return "", fmt.Errorf("failed to download file: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("failed to download file: HTTP %d", resp.StatusCode)
	}

	// Create local file
	file, err := os.Create(filepath)
	if err != nil {
		return "", fmt.Errorf("failed to create local file: %w", err)
	}
	defer file.Close()

	// Copy content
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return "", fmt.Errorf("failed to save file: %w", err)
	}

	return filepath, nil
}

func (fs *FileSplitter) splitFileInMemory(filepath string, numParts int) (*SplitFile, error) {
	// Open the file
	file, err := os.Open(filepath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Get file info
	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileSize := info.Size()
	if fileSize == 0 {
		return nil, fmt.Errorf("file is empty")
	}

	// Memory map the file for O(1) access
	data, err := syscall.Mmap(int(file.Fd()), 0, int(fileSize), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		return nil, fmt.Errorf("failed to mmap file: %w", err)
	}
	defer syscall.Munmap(data)

	// Convert to string view for easier handling
	text := *(*string)(unsafe.Pointer(&data))

	// Calculate approximate chunk size
	chunkSize := int(fileSize) / numParts

	// Find split points that don't cut words
	splitPoints := make([]int, 0, numParts-1)

	for i := 1; i < numParts; i++ {
		targetPos := i * chunkSize
		pos := findWordBoundary(text, targetPos)
		if pos > 0 && pos < len(text) {
			splitPoints = append(splitPoints, pos)
		}
	}

	// Create parts metadata
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

	// Add last part
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

	// Look forward for whitespace (word boundary)
	for i := targetPos; i < len(text); i++ {
		if unicode.IsSpace(rune(text[i])) {
			// Skip consecutive whitespace to start next chunk with non-space
			for i < len(text) && unicode.IsSpace(rune(text[i])) {
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

	// Read the specific part directly
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

	// Generate job ID from URL hash
	jobID := fmt.Sprintf("%x", req.URL)[0:8]

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
		os.Remove(filepath) // Clean up
		http.Error(w, fmt.Sprintf("Split failed: %v", err), http.StatusInternalServerError)
		return
	}

	splitFile.URL = req.URL
	fs.files[jobID] = splitFile

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

func (fs *FileSplitter) handleStatus(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	jobID := vars["jobId"]

	fs.mu.RLock()
	splitFile, exists := fs.files[jobID]
	fs.mu.RUnlock()

	if !exists {
		http.Error(w, "Job not found", http.StatusNotFound)
		return
	}

	response := SplitResponse{
		JobID:    jobID,
		URL:      splitFile.URL,
		NumParts: splitFile.NumParts,
		Parts:    splitFile.Parts,
		FileSize: splitFile.FileSize,
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(response)
}

func (fs *FileSplitter) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]string{"status": "healthy"})
}

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}

	fs := NewFileSplitter()

	r := mux.NewRouter()

	// API routes
	r.HandleFunc("/split", fs.handleSplit).Methods("POST")
	r.HandleFunc("/job/{jobId}/part/{partNum}", fs.handleGetPart).Methods("GET")
	r.HandleFunc("/job/{jobId}/status", fs.handleStatus).Methods("GET")
	r.HandleFunc("/health", fs.handleHealth).Methods("GET")

	// Add some basic documentation
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Write([]byte(`
<!DOCTYPE html>
<html>
<head><title>File Splitter API</title></head>
<body>
<h1>File Splitter API</h1>
<h2>Endpoints:</h2>
<ul>
<li><strong>POST /split</strong> - Split a file from S3 URL
<pre>{"url": "https://s3.amazonaws.com/.../file.txt", "num_parts": 4}</pre>
</li>
<li><strong>GET /job/{jobId}/part/{partNum}</strong> - Get a specific part</li>
<li><strong>GET /job/{jobId}/status</strong> - Get job status and part info</li>
<li><strong>GET /health</strong> - Health check</li>
</ul>
</body>
</html>
		`))
	})

	log.Printf("Starting server on port %s", port)
	log.Fatal(http.ListenAndServe(":"+port, r))
}
