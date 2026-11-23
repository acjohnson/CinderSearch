BINARY      := bin/cindersearch
CMD_PKG     := ./cmd/cindersearch
VERSION     := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
COMMIT      := $(shell git rev-parse --short HEAD 2>/dev/null || echo "local")
LDFLAGS     := -s -w -X main.version=$(VERSION) -X main.commit=$(COMMIT)

GO          ?= go
GOFLAGS     ?=
RACE_FLAG   := -race

.PHONY: all build test test-v race bench bench-live profile-cpu profile-mem demo run docker-build helm-lint helm-template fmt fmt-check vet lint tidy clean help

all: vet test build

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

## bench: Run Go microbenchmarks for CRC32C, WAL, NVMe, segment, query, and HTTP
bench:
	$(GO) test -run=^$$ -bench=. -benchmem ./...

## bench-live: Build binary and run end-to-end throughput/latency benchmark
bench-live: build
	./$(BINARY) bench --docs=5000 --batch-size=250 --concurrency=8 --queries=1000

## profile-cpu: Run load benchmark with CPU pprof output and top hotspots summary
profile-cpu: build
	./$(BINARY) bench --docs=10000 --batch-size=250 --concurrency=8 --queries=2000 --cpuprofile=/tmp/cindersearch-cpu.pprof
	$(GO) tool pprof -top -cum /tmp/cindersearch-cpu.pprof | head -n 30

## profile-mem: Run load benchmark with Heap pprof output and top allocs summary
profile-mem: build
	./$(BINARY) bench --docs=10000 --batch-size=250 --concurrency=8 --queries=2000 --memprofile=/tmp/cindersearch-mem.pprof
	$(GO) tool pprof -top -alloc_space /tmp/cindersearch-mem.pprof | head -n 30

## demo: Build and run the end-to-end 3-node S3 + NVMe + Bit-Rot self-healing demo
demo: build
	./$(BINARY) demo

## run: Build and start a local single-node CinderSearch server with embedded S3 on :9200
run: build
	./$(BINARY) serve --node-id node-1 --listen :9200 --embedded-s3 --nvme-dir /tmp/cindersearch-nvme

## docker-build: Build production CinderSearch container image
docker-build:
	docker build --build-arg VERSION=$(VERSION) --build-arg COMMIT=$(COMMIT) -t ghcr.io/cindersearch/cindersearch:$(VERSION) .

## helm-lint: Validate the Kubernetes Helm chart and example topologies
helm-lint:
	helm lint ./charts/cindersearch
	helm lint ./charts/cindersearch -f ./charts/cindersearch/examples/values-single-dev.yaml
	helm lint ./charts/cindersearch -f ./charts/cindersearch/examples/values-multimaster-minio.yaml
	helm lint ./charts/cindersearch -f ./charts/cindersearch/examples/values-disaggregated-aws-s3.yaml

## helm-template: Render Helm manifests across all deployment presets
helm-template:
	helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-single-dev.yaml >/dev/null
	helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-multimaster-minio.yaml >/dev/null
	helm template cindersearch ./charts/cindersearch -f ./charts/cindersearch/examples/values-disaggregated-aws-s3.yaml >/dev/null
	@echo "All Helm chart topologies rendered cleanly"

## fmt: Format all Go source files in-place (opt-in)
fmt:
	gofmt -s -w cmd internal

## fmt-check: Check Go formatting without modifying files
fmt-check:
	@test -z "$$(gofmt -l cmd internal)" || (echo "Unformatted files:"; gofmt -l cmd internal; exit 1)

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
