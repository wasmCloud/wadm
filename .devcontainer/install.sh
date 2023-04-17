#!/bin/bash

cargo install wash-cli --git https://github.com/wasmcloud/wash --branch feat/wadm_0.4_support --force 
cargo install wadm --bin wadm --features cli --git https://github.com/wasmcloud/wadm --branch wadm_0.4 --force