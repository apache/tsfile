#!/bin/bash

# Remote host configuration
REMOTE_HOST="shuolin@Fit"
REMOTE_PATH="${REMOTE_PATH:-/home/shuolin/tsfile_b1}"

# Resolve project root relative to this script (cpp/experiment/../../ = tsfile_b1/)
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# Use Homebrew rsync to avoid macOS openrsync SIGSEGV bug with large file sets
RSYNC=$(command -v /opt/homebrew/bin/rsync || command -v /usr/local/bin/rsync || echo rsync)

# Sync the entire tsfile project (not just cpp/) to remote, excluding build artifacts
# and experiment output data (CSV results, plots, generated data files)
"$RSYNC" -avz --progress \
  --exclude='cmake-build*/' \
  --exclude='build/' \
  --exclude='target/' \
  --exclude='.cache/' \
  --exclude='*.o' \
  --exclude='*.a' \
  --exclude='*.so' \
  --exclude='*.dylib' \
  --exclude='*.exe' \
  --exclude='CMakeFiles/' \
  --exclude='CMakeCache.txt' \
  --exclude='cmake_install.cmake' \
  --exclude='CTestTestfile.cmake' \
  --exclude='Makefile' \
  --exclude='*.csv' \
  --exclude='*.pdf' \
  --exclude='*.png' \
  --exclude='*.tsfile' \
  --exclude='*.parquet' \
  --exclude='*.dat' \
  --exclude='cpp/experiment/**/datasets/prepared/' \
  --exclude='cpp/experiment/**/datasets/raw/' \
  "${PROJECT_ROOT}/" "${REMOTE_HOST}:${REMOTE_PATH}/"

echo "Sync complete: ${PROJECT_ROOT}/ -> ${REMOTE_HOST}:${REMOTE_PATH}/"
