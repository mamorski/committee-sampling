# Build stage
FROM golang:1.23-alpine AS builder

# Install protobuf compiler and make
RUN apk add --no-cache protobuf-dev make

# Install Go protobuf plugins
RUN go install google.golang.org/protobuf/cmd/protoc-gen-go@latest && \
    go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Generate proto files and build
RUN make proto && go build -o committee-sampling ./cmd/committee-sampling

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS requests
RUN apk --no-cache add ca-certificates

# Create non-root user
RUN adduser -D -s /bin/sh appuser

# Set working directory
WORKDIR /app

# Copy binary from builder stage
COPY --from=builder /app/committee-sampling .

# Copy config files
COPY --from=builder /app/configs ./configs

# Change ownership to non-root user
RUN chown -R appuser:appuser /app
USER appuser

# Expose port (adjust based on your application's needs)
EXPOSE 8080

# Run the binary
CMD ["./committee-sampling"] 