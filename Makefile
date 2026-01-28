.PHONY: fmt lint test

test:
	@go test -race ./...

lint:
	@golangci-lint run

fmt:
	@gofumpt -l -w .