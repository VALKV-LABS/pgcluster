.DEFAULT_GOAL := help

COMPOSE_INTEG  = docker/integ-compose.yml
COMPOSE_E2E    = docker/e2e-compose.yml
COMPOSE_SHARD  = docker/shard-e2e-compose.yml
CARGO          = cargo
CARGO_FLAGS    =

##@ Help

.PHONY: help
help:  ## Show this help message
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} \
	/^[a-zA-Z_-]+:.*?##/ { printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2 } \
	/^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) }' $(MAKEFILE_LIST)

##@ Build

.PHONY: build build-release clean fmt check

build:  ## Build all workspace crates (debug)
	$(CARGO) build --workspace $(CARGO_FLAGS)

build-release:  ## Build release binaries
	$(CARGO) build --workspace --release $(CARGO_FLAGS)

clean:  ## Remove build artifacts
	$(CARGO) clean

fmt:  ## Format all code with cargo fmt
	$(CARGO) fmt --all

check:  ## Run clippy lints (warnings as errors)
	$(CARGO) clippy --workspace --all-targets -- -D warnings

##@ Unit Tests (no Docker required)

.PHONY: test test-unit

test: test-unit  ## Alias for test-unit

test-unit:  ## Run all unit tests locally (fast, no Docker)
	$(CARGO) test --workspace $(CARGO_FLAGS)

##@ Integration Tests (requires Docker)

.PHONY: test-integ integ-up integ-down

integ-up:  ## Build images, start services, run all tests inside Docker (services stay up)
	docker compose -f $(COMPOSE_INTEG) up -d --wait --build postgres vk-agent pgcluster
	docker compose -f $(COMPOSE_INTEG) run --rm --build test-runner

integ-down:  ## Tear down integration test Docker environment
	docker compose -f $(COMPOSE_INTEG) down -v --remove-orphans

test-integ:  ## Build, run all tests inside Docker, then tear down
	docker compose -f $(COMPOSE_INTEG) up -d --wait --build postgres vk-agent pgcluster && \
	docker compose -f $(COMPOSE_INTEG) run --rm --build test-runner; \
	EXIT=$$?; \
	docker compose -f $(COMPOSE_INTEG) down -v --remove-orphans; \
	exit $$EXIT

##@ E2E Stack — full 3-node pgcluster cluster

.PHONY: start stop logs status restart

start:  ## Start the full pgcluster + vk-agent + Postgres 3-node stack
	docker compose -f $(COMPOSE_E2E) up -d --build --wait

stop:  ## Stop and remove the full stack (deletes volumes)
	docker compose -f $(COMPOSE_E2E) down -v --remove-orphans

restart: stop start  ## Rebuild and restart the full stack

logs:  ## Tail logs from all containers in the full stack
	docker compose -f $(COMPOSE_E2E) logs -f

status:  ## Show container health and ports for the full stack
	docker compose -f $(COMPOSE_E2E) ps

##@ Multi-Shard E2E Stack (M5-A — coordinator + 2 shards)

.PHONY: shard-start shard-stop shard-logs shard-status

shard-start:  ## Start coordinator + 2 shard pgcluster clusters (multi-shard E2E)
	docker compose -f $(COMPOSE_SHARD) up -d --build --wait

shard-stop:  ## Stop and remove the multi-shard stack
	docker compose -f $(COMPOSE_SHARD) down -v --remove-orphans

shard-logs:  ## Tail logs from the multi-shard stack
	docker compose -f $(COMPOSE_SHARD) logs -f

shard-status:  ## Show container health for the multi-shard stack
	docker compose -f $(COMPOSE_SHARD) ps

##@ Convenience

.PHONY: all ci setup-hooks

all: fmt check test  ## Format, lint, then unit test

ci: check test test-integ  ## Full CI pipeline (lint + unit + integ)

setup-hooks:  ## Install git hooks — run once after cloning (enables pre-push fmt+clippy)
	git config core.hooksPath .githooks
	chmod +x .githooks/pre-push
