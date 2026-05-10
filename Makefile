.PHONY: test test-race

# Run tests for local modules defined in go.work
test:
	go test -v $$(go list -m -f '{{.Dir}}/...')

# Run tests with race detector for local modules
test-race:
	go test -v -race $$(go list -m -f '{{.Dir}}/...')

# Run tests with coverage profile and race detector
test-coverage:
	go test -v -race -coverprofile=coverage.out $$(go list -m -f '{{.Dir}}/...')
	go tool cover -func=coverage.out
	@rm coverage.out
