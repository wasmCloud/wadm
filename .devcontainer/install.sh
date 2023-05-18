#!/bin/bash

# INSTALL WASH
curl -s https://packagecloud.io/install/repositories/wasmcloud/core/script.deb.sh | sudo bash
sudo apt install wash openssl -y