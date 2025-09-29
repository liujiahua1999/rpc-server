# Use official Go image as base
FROM golang:1.21-alpine AS builder

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum* ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o worker main.go

# Final stage - use minimal image
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Set working directory
WORKDIR /root/

# Copy the binary from builder stage
COPY --from=builder /app/worker .

# Expose port (if needed for health checks)
EXPOSE 8080

# Set default environment variables for remote RPC server
ENV WORKER_ID=""
ENV RPC_SERVER_ADDR="98.82.151.62:1234"
ENV REPORT_BACK_URL="http://98.82.151.62:8080/results"
ENV POLL_INTERVAL="30s"

# Run the binary
CMD ["./worker"]