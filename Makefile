.DEFAULT: all

all: build test

build:
	cargo build

ifeq ($(shell nc -czt -w1 127.0.0.1 4222 || echo fail),fail)
test::
	docker run --rm -d --name wadm-test -p 127.0.0.1:4222:4222 nats:2.9 -js
	cargo test -- --nocapture
	docker stop wadm-test
else
test::
	cargo test -- --nocapture
endif

.PHONY: test build all