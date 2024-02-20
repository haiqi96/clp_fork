#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

pip3 install fusepy
pip3 install diskcache
pip3 install s3path
pip3 install boto3
