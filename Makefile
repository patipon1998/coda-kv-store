.DEFAULT_GOAL := help
.PHONY: help build test race cover lint fmt counter bench clean \
        docker-build up-part1 up-part2 down demo-part1 demo-part2 e2e

GO      ?= go
BIN     := bin
COMPOSE := docker-compose

help: ## Show this help
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN{FS=":.*?## "}{printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

# --- build ------------------------------------------------------------------

build: ## Build both binaries into ./bin
	@mkdir -p $(BIN)
	$(GO) build -trimpath -o $(BIN)/node  ./cmd/node
	$(GO) build -trimpath -o $(BIN)/proxy ./cmd/proxy

# --- test -------------------------------------------------------------------

test: ## Run all tests
	$(GO) test ./...

race: ## Run all tests under the race detector — THIS is the real gate
	$(GO) test -race ./...

cover: ## Run tests with a coverage report
	$(GO) test -race -coverprofile=coverage.out ./...
	$(GO) tool cover -func=coverage.out | tail -1
	@echo "html report: go tool cover -html=coverage.out"

counter: ## Run the 300-counter, with and without CAS
	$(GO) test -race -v -run 'TestConcurrentCounter' ./internal/store/

bench: ## Run benchmarks, including partition-count scaling
	$(GO) test -bench . -benchmem -run '^$$' ./internal/...

lint: fmt ## go vet plus a gofmt check
	$(GO) vet ./...

openapi: ## Validate the OpenAPI spec
	docker run --rm -v "$$PWD/docs:/spec" python:3.12-alpine sh -c \
		"pip install -q openapi-spec-validator && \
		 python -c \"from openapi_spec_validator import validate; \
		 from openapi_spec_validator.readers import read_from_filename; \
		 s,_=read_from_filename('/spec/openapi.yaml'); validate(s); print('OpenAPI 3.1 valid')\""

fmt: ## Report unformatted files
	@out=$$(gofmt -l .); if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

# --- docker -----------------------------------------------------------------

docker-build: ## Build node and proxy images
	$(COMPOSE) -f deploy/compose.part2.yml build

up-part1: ## Start the Part 1 single node
	$(COMPOSE) -f deploy/compose.part1.yml up -d --build

up-part2: ## Start the Part 2 proxy plus three nodes
	$(COMPOSE) -f deploy/compose.part2.yml up -d --build

down: ## Stop and remove everything
	-$(COMPOSE) -f deploy/compose.part1.yml down -v
	-$(COMPOSE) -f deploy/compose.part2.yml down -v

# --- demo -------------------------------------------------------------------

demo-part1: ## Part 1 demo: the assignment's curl examples plus the counter
	./scripts/demo-part1.sh

demo-part2: ## Part 2 demo: routing, GET /kv, node failure, misroute
	./scripts/demo-part2.sh

throughput: ## Measure write throughput against a live stack (see README)
	KV_BASE_URL=$${KV_BASE_URL:-http://localhost:$${HOST_PORT:-8081}} \
		$(GO) test -count=1 -tags e2e -v ./test/e2e/ -run TestThroughput

e2e: ## End-to-end tests against a live stack (needs make up-part2)
	KV_BASE_URL=$${KV_BASE_URL:-http://localhost:$${HOST_PORT:-8081}} \
		$(GO) test -count=1 -tags e2e -v ./test/e2e/

# --- housekeeping -----------------------------------------------------------

clean: down ## Remove binaries, coverage output and containers
	rm -rf $(BIN) coverage.out
