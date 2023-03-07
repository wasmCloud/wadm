.DEFAULT: all

all: build test

build:
	cargo build --features cli

ifeq ($(shell nc -czt -w1 127.0.0.1 4222 || echo fail),fail)
test::
	docker run --rm -d --name wadm-test -p 127.0.0.1:4222:4222 nats:2.9 -js
	$(MAKE) test
	docker stop wadm-test
else
test:: _test
endif

# Internal test target for reuse
_test:
	cargo test -- --nocapture

.PHONY: test build all