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
CARGO_WATCH ?= cargo-watch
CARGO_CLIPPY ?= cargo-clippy
DOCKER ?= docker

all: build test

help:  ## Display this help
	@awk 'BEGIN {FS = ":.*##"; printf "\nUsage:\n  make \033[36m<target>\033[0m\n"} /^[a-zA-Z_\-.*]+:.*?##/ { printf "  \033[36m%-15s\033[0m %s\n", $$1, $$2 } /^##@/ { printf "\n\033[1m%s\033[0m\n", substr($$0, 5) } ' $(MAKEFILE_LIST)

###########
# Tooling #
###########

# Ensure that cargo watch is installed
check-cargo-watch:
ifeq ("",$(shell command -v $(CARGO_WATCH)))
	$(error "ERROR: cargo-watch is not installed (see: https://crates.io/crates/cargo-watch)")
endif

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

lint-watch: check-cargo-clippy ## Run code lint (continuously)
	$(CARGO) watch -- $(MAKE) lint

build: ## Build wadm
	$(CARGO) build

build-watch: check-cargo-watch ## Build wadm (continuously)
	$(CARGO) watch -- $(MAKE) build

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

test-watch: ## Run tests (continuously)
	$(CARGO) watch -- $(MAKE) test

test-int:: ## Run integration tests
ifeq (,$(CARGO_TEST_TARGET))
	$(CARGO) test -- --nocapture
else
	$(CARGO) test --test $(CARGO_TEST_TARGET) -- --nocapture
endif

test-int-watch: ## Run integration tests (continuously)
	$(CARGO) watch -- $(MAKE) test-int

test-int-all:: ## Run all integration tests
	$(MAKE) test-int CARGO_TEST_TARGET='*'

.PHONY: check-cargo-watch check-cargo-clippy lint build build-watch test test-watch test-int test-int-all test-int-watch
