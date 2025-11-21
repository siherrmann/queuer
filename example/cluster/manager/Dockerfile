# Build stage
FROM golang:1.25-alpine AS builder

# Install git and ca-certificates for fetching dependencies
RUN apk add --no-cache git ca-certificates

# Set working directory
WORKDIR /app

# Copy go.mod and go.sum for dependency caching
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy entire project source code
COPY . .

# Build the CLI application
RUN CGO_ENABLED=0 GOOS=linux go build -a -installsuffix cgo -o cli ./cli

# Final stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN addgroup -g 1001 appgroup && \
    adduser -D -u 1001 -G appgroup appuser

WORKDIR /app

# Copy the CLI binary from builder stage
COPY --from=builder /app/cli .

# Change ownership to non-root user
RUN chown appuser:appgroup cli

# Switch to non-root user
USER appuser

# Expose port (default 8080, can be overridden)
EXPOSE 8080

# Use the CLI to start the manager server
CMD ["./cli", "manager", "start", "--port", "8080"]