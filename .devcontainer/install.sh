#!/bin/bash

# INSTALL WADM
apt update; apt install curl -y

ARCH=$(arch)
VERSION=v0.4.0-alpha.1

if [[ $ARCH == "arm64" ]]; then
    ARCH="aarch64"
fi
if [[ $ARCH == "x86_64" ]]; then
    ARCH="amd64"
fi

TARBALL=wadm-$VERSION-linux-$ARCH.tar.gz

curl -fLO https://github.com/wasmCloud/wadm/releases/download/$VERSION/$TARBALL
tar -xvf $TARBALL
mv $TARBALL/wadm /usr/local/bin/wadm

# INSTALL WASH
cargo install wash-cli --git https://github.com/wasmcloud/wash --branch feat/wadm_0.4_support --force 