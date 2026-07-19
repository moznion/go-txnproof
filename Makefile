.PHONY: fmt lint test

fmt:
	gofmt -s -w .
	go tool -modfile=internal/tools/go.mod goimports -w .

lint:
	go tool -modfile=internal/tools/go.mod golangci-lint run

test:
	go test -race -v ./...

