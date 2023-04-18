#!/bin/bash

# INSTALL WADM
ARCH=$(arch)
VERSION=v0.4.0-alpha.1

if [[ $ARCH == "x86_64" ]]; then
    ARCH="amd64"
fi

TARBALL=wadm-$VERSION-linux-$ARCH

curl -fLO https://github.com/wasmCloud/wadm/releases/download/$VERSION/$TARBALL.tar.gz
tar -xvf $TARBALL.tar.gz
mv $TARBALL/wadm /usr/local/bin/wadm
rm -rf $TARBALL $TARBALL.tar.gz

# INSTALL WASH
cargo install wash-cli --git https://github.com/wasmcloud/wash --branch feat/wadm_0.4_support --force 
