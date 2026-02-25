.PHONY: fmt lint test

test:
	@go test -v -race -count 3 ./...

lint:
	@golangci-lint run

fmt:
	@gofumpt -l -w .