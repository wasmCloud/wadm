#!/bin/bash

cargo install wash-cli --git https://github.com/wasmcloud/wash --branch feat/wadm_0.4_support --force 
cargo install wadm --bin wadm --features cli --git https://github.com/wasmcloud/wadm --tag v0.4.0-alpha.1 --force