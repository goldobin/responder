.PHONY: fmt lint test

test:
	@go test -v -race ./...

lint:
	@golangci-lint run

fmt:
	@gofumpt -l -w .