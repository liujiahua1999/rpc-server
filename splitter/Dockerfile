# Use official Go runtime as base image
FROM golang:1.22-alpine AS builder

# Set working directory
WORKDIR /app

# Install git (needed for go mod download)
RUN apk add --no-cache git

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY rpc-server.go .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o splitter-server rpc-server.go

# Use minimal alpine image for runtime
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create app directory
WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/splitter-server .

# Create temp directory for file downloads
RUN mkdir -p /tmp/splitter

# Expose port
EXPOSE 8080

# Set environment variable for port
ENV PORT=8080

# Run the application
CMD ["./splitter-server"]