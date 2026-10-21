# GRANITE — Master Makefile

.PHONY: all build test test-race proto clean help bench cluster

# Default target
all: build test

## proto: Compile protocol buffer files into Go code
proto:
	@echo "Compiling Protocol Buffers..."
	@protoc --go_out=. --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative proto/raft.proto
	@echo "Done."

## build: Build all server and benchmark binaries
build:
	@echo "Building GRANITE binaries..."
	@go build -o bin/server.exe ./cmd/server
	@go build -o bin/benchmark.exe ./cmd/benchmark
	@echo "Binaries built in bin/"

## test: Run all unit and integration tests across storage, rpc, and raft
test:
	@echo "Running GRANITE test suite..."
	@go test ./internal/... -v

## test-race: Run tests with Go data race detector enabled
test-race:
	@echo "Running race detector..."
	@go test ./internal/... -race -v

## bench: Run storage engine benchmark suite
bench:
	@go run ./cmd/benchmark --ops 5000

## clean: Clean compiled binaries and test data directories
clean:
	@echo "Cleaning binaries and data directories..."
	@rm -rf bin/ data/ ./data/bench
	@echo "Clean complete."

## help: Print this help message
help:
	@echo "GRANITE Build System Tasks:"
	@echo "  make proto      - Recompile .proto files"
	@echo "  make build      - Build binaries"
	@echo "  make test       - Run all unit & integration tests"
	@echo "  make test-race  - Run tests with race detector"
	@echo "  make bench      - Run performance benchmark"
	@echo "  make clean      - Clean build artifacts"
