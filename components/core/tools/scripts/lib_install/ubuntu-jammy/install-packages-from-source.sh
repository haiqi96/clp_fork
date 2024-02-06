#!/usr/bin/env bash

# Exit on any error
set -e

# Error on undefined variable
set -u

script_dir="$( cd "$( dirname "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

"$script_dir"/../fmtlib.sh 8.0.1
"$script_dir"/../libarchive.sh 3.5.1
"$script_dir"/../lz4.sh 1.8.2
"$script_dir"/../mongoc.sh 1.24.4
"$script_dir"/../mongocxx.sh 3.8.0
"$script_dir"/../msgpack.sh 6.0.0
"$script_dir"/../spdlog.sh 1.9.2
"$script_dir"/../zstandard.sh 1.4.9
