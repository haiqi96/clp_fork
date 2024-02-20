#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install --no-install-recommends -y \
  checkinstall \
  curl \
  fuse \
  libmariadb-dev \
  libssl-dev \
  libspdlog-dev \
  python3 \
  python3-pip \
  rsync \
  zstd
