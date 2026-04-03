#!/bin/bash

# Remote host configuration
REMOTE_HOST="shuolin@Fit"
REMOTE_PATH="${REMOTE_PATH:-/home/shuolin/tsfile_memory}"

# Use Homebrew rsync to avoid macOS openrsync SIGSEGV bug with large file sets
RSYNC=$(command -v /opt/homebrew/bin/rsync || command -v /usr/local/bin/rsync || echo rsync)

# Sync source code to remote, excluding build artifacts
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
  ../ "${REMOTE_HOST}:${REMOTE_PATH}/"

echo "Sync complete: ../ -> ${REMOTE_HOST}:${REMOTE_PATH}/"
