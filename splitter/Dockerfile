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
COPY server.go .

# Build the application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o server server.go

# Use minimal alpine image for runtime
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create app directory
WORKDIR /root/

# Copy binary from builder
COPY --from=builder /app/server .

# Expose ports for HTTP and RPC
EXPOSE 8080 1234

# Set environment variables
ENV HTTP_PORT=8080
ENV RPC_PORT=1234

# Run the application
CMD ["./server"]