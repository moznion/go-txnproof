.PHONY: fmt lint test fuzz

# Time budget per fuzz target. The default keeps a full sweep a few minutes;
# raise it for a deeper local run: make fuzz FUZZTIME=5m
FUZZTIME ?= 30s

fmt:
	gofmt -s -w .
	go tool -modfile=internal/tools/go.mod goimports -w .

lint:
	go tool -modfile=internal/tools/go.mod golangci-lint run

test:
	go test -race -v ./...

# Runs every fuzz target for FUZZTIME each. `make test` already replays the
# seed corpora and any committed crashers; this is the target that goes
# looking for new ones.
fuzz:
	@set -e; \
	for pkg in . ./crosscheck ./pgcheck ./mycheck; do \
		for target in $$(go test -list '^Fuzz' $$pkg | grep '^Fuzz'); do \
			echo "==> $$pkg $$target ($(FUZZTIME))"; \
			go test $$pkg -run '^$$' -fuzz "^$$target$$" -fuzztime $(FUZZTIME); \
		done; \
	done

