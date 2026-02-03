#!/bin/bash
# Run Java-C# Interoperability Tests
# This script generates Java test files and runs C# validation tests

set -e

# Configuration
TEST_FILES_DIR="/tmp/interop-test-files"

echo "========================================="
echo "TSFile Interoperability Test Suite"
echo "========================================="
echo ""

# Step 1: Build and run Java generator
echo "Step 1: Building Java test generator..."
cd "$(dirname "$0")/java/interop-tests" || exit 1
mvn clean install -DskipTests
echo ""

echo "Step 2: Generating test files..."
mvn exec:java

FILE_COUNT=$(find "$TEST_FILES_DIR" -name "*.tsfile" 2>/dev/null | wc -l)
echo "Generated $FILE_COUNT test files"
echo ""

# Step 2: Run C# tests
echo "Step 3: Running C# interoperability tests..."
cd "$(dirname "$0")/csharp/tests/Apache.TsFile.InteropTests" || exit 1
dotnet test --logger "console;verbosity=normal"

echo ""
echo "========================================="
echo "Test run complete!"
echo "========================================="
