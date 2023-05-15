SHELL := bash
.ONESHELL:
.SHELLFLAGS := -eu -o pipefail -c
.DELETE_ON_ERROR:
MAKEFLAGS += --warn-undefined-variables
MAKEFLAGS += --no-builtin-rules
MAKEFLAGS += --no-print-directory
MAKEFLAGS += -S

.DEFAULT: all

CARGO ?= cargo
CARGO_CLIPPY ?= cargo-clippy
DOCKER ?= docker
NATS ?= nats

all: build test

help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_\-.*]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

###########
# Tooling #
###########

# Ensure that clippy is installed
check-cargo-clippy:
ifeq ("",$(shell command -v $(CARGO_CLIPPY)))
	$(error "ERROR: clippy is not installed (see: https://doc.rust-lang.org/clippy/installation.html)")
endif

#########
# Build #
#########

lint: check-cargo-clippy ## Run code lint
	$(CARGO) fmt --all --check
	$(CARGO) clippy --all-features --all-targets --workspace

build: ## Build wadm
	$(CARGO) build --bin wadm --features cli

########
# Test #
########

# An optional specific test for cargo to target

CARGO_TEST_TARGET ?=

test:: ## Run tests
ifeq ($(shell nc -czt -w1 127.0.0.1 4222 || echo fail),fail)
	$(DOCKER) run --rm -d --name wadm-test -p 127.0.0.1:4222:4222 nats:2.9 -js
	$(CARGO) test $(CARGO_TEST_TARGET) -- --nocapture
	$(DOCKER) stop wadm-test
else
	$(CARGO) test $(CARGO_TEST_TARGET) -- --nocapture
endif

test-e2e::
ifeq ($(shell nc -czt -w1 127.0.0.1 4222 || echo fail),fail)
	@$(MAKE) build
	$(CARGO) test --test e2e_multiple_hosts --features _e2e_tests --  --nocapture 
else
	@echo "WARN: Not running e2e tests. NATS must not be currently running"
	exit 1
endif

###########
# Cleanup #
###########

stream-cleanup: ## Purges all streams that wadm creates
	$(NATS) stream purge wadm_commands --force
	$(NATS) stream purge wadm_events --force
	$(NATS) stream purge wadm_notify --force
	$(NATS) stream purge wadm_mirror --force
	$(NATS) stream purge KV_wadm_state --force
	$(NATS) stream purge KV_wadm_manifests --force

.PHONY: check-cargo-clippy lint build build-watch test stream-cleanup clean test-e2e test 
