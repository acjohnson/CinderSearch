BINARY      := bin/cindersearch
CMD_PKG     := ./cmd/cindersearch
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      := $(shell git rev-parse --short HEAD 2>/dev/null || echo "local")
LDFLAGS     := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)

GO          ?= go
GOFLAGS     ?=
RACE_FLAG   := -race

.PHONY: all build test test-v race bench demo run fmt vet lint tidy clean help

all: fmt vet test build

## build: Compile the CinderSearch binary to bin/cindersearch
build:
	@mkdir -p bin
	$(GO) build $(GOFLAGS) -ldflags "$(LDFLAGS)" -o $(BINARY) $(CMD_PKG)
	@echo "Built $(BINARY) ($(VERSION))"

## test: Run all unit and integration tests with race detector
test:
	$(GO) test $(RACE_FLAG) -count=1 ./...

## test-v: Run all tests with verbose output
test-v:
	$(GO) test $(RACE_FLAG) -v -count=1 ./...

## race: Alias for race-enabled test suite
race: test

## bench: Run Go microbenchmarks for CRC32C, indexing, and search
bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./...

## demo: Build and run the end-to-end 3-node S3 + NVMe + Bit-Rot self-healing demo
demo: build
	./$(BINARY) demo

## run: Build and start a local single-node CinderSearch server with embedded S3 on :9200
run: build
	./$(BINARY) serve --node-id node-1 --listen :9200 --embedded-s3 --nvme-dir /tmp/cindersearch-nvme

## fmt: Format all Go source files
fmt:
	gofmt -s -w cmd internal

## vet: Run go vet static analysis
vet:
	$(GO) vet ./...

## tidy: Verify go.mod and go.sum
tidy:
	$(GO) mod tidy

## clean: Remove build artifacts and temporary NVMe demo caches
clean:
	rm -rf bin /tmp/cindersearch-nvme /tmp/cindersearch-demo-*

## help: Show available Makefile targets
help:
	@grep -E '^## ' $(MAKEFILE_LIST) | sed 's/## //' | column -t -s ':'
