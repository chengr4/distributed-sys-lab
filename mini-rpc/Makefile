.PHONY: build run test test-race clean help

BINARY_NAME=node_app

# Default target: help
help:
	@echo "Usage:"
	@echo "  make build         - Compile the project and create the binary '$(BINARY_NAME)'"
	@echo "  make run PORT=1234 - Build and run a node on the specified port (default: 1234)"
	@echo "  make test          - Run all unit and integration tests"
	@echo "  make test-race     - Run tests with Go race detector enabled"
	@echo "  make clean         - Remove binary and log files"

# Build the project
build:
	go build -o $(BINARY_NAME) .

# Run a node (Usage: make run PORT=1235)
PORT ?= 1234
run: build
	./$(BINARY_NAME) -port $(PORT)

# Run all tests
test:
	go test -v ./...

# Run tests with race detector
test-race:
	go test -v -race ./...

# Cleanup build artifacts and logs
clean:
	rm -f $(BINARY_NAME)
	rm -f *.log
	rm -f tests/*.log
