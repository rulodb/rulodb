#!/bin/bash

# Integration test runner script for RuloDB
# This script starts the server, runs the integration tests, then cleans up

set -euo pipefail

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

SERVER_LOG="server.log"
SERVER_PID=""

echo -e "${YELLOW}=== RuloDB Integration Test Runner ===${NC}"

# Build the project first
echo -e "${YELLOW}Building RuloDB...${NC}"
cargo build --release

# Clean up any existing data and processes
echo -e "${YELLOW}Cleaning up existing data and processes...${NC}"
pkill -f "rulodb start" 2>/dev/null || true
rm -rf rulodb_data 2>/dev/null || true
rm -f $SERVER_LOG 2>/dev/null || true
sleep 1

# Function to cleanup on exit
cleanup() {
    echo -e "${YELLOW}Cleaning up...${NC}"
    if [ ! -z "${SERVER_PID}" ]; then
        kill $SERVER_PID 2>/dev/null || true
    fi
    pkill -f "rulodb start" 2>/dev/null || true
    rm -f $SERVER_LOG 2>/dev/null || true
    echo -e "${GREEN}Cleanup complete.${NC}"
}

# Set trap to cleanup on script exit
trap cleanup EXIT

# Start the server in background
echo -e "${YELLOW}Starting RuloDB server...${NC}"
RUST_LOG=info ./target/release/rulodb start > $SERVER_LOG 2>&1 &
SERVER_PID=$!

# Wait for server to start
echo -e "${YELLOW}Waiting for server to start...${NC}"
sleep 3

# Check if server is running
if ! kill -0 $SERVER_PID 2>/dev/null; then
    echo -e "${RED}Server failed to start. Check server.log for details:${NC}"
    cat $SERVER_LOG 2>/dev/null || echo "No server log found"
    exit 1
fi

# Check if server is listening on port
echo -e "${YELLOW}Checking if server is listening on port 6090...${NC}"
for i in {1..30}; do
    if nc -z 127.0.0.1 6090 2>/dev/null; then
        echo -e "${GREEN}Server is ready!${NC}"
        break
    fi
    if [ "${i}" -eq "30" ]; then
        echo -e "${RED}Server is not responding on port 6090 after 30 attempts${NC}"
        echo -e "${RED}Server log:${NC}"
        cat $SERVER_LOG 2>/dev/null || echo "No server log found"
        exit 1
    fi
    echo -e "${YELLOW}Waiting for server to be ready... (attempt ${i}/30)${NC}"
    sleep 1
done

# Run integration tests
echo -e "${YELLOW}Running integration tests...${NC}"

# Check if integration test files exist
if ! ls tests/integration_*.rs >/dev/null 2>&1; then
    echo -e "${RED}No integration test files found matching tests/integration_*.rs${NC}"
    exit 1
fi

# Initialize test tracking
TOTAL_TESTS=0
PASSED_TESTS=0
FAILED_TESTS=0

# Run each integration test individually
for test_file in tests/integration_*.rs; do
    if [ -f "$test_file" ]; then
        test_name=$(basename "${test_file}" .rs)
        echo -e "${YELLOW}Running ${test_name}...${NC}"

        TOTAL_TESTS=$((TOTAL_TESTS + 1))

        if RUST_LOG=info cargo test --test "${test_name}" -- --test-threads=1 2>&1; then
            echo -e "${GREEN}✓ ${test_name} passed${NC}"
            PASSED_TESTS=$((PASSED_TESTS + 1))
        else
            echo -e "${RED}✗ ${test_name} failed${NC}"
            FAILED_TESTS=$((FAILED_TESTS + 1))
            # Don't exit immediately, continue with other tests
        fi
        echo ""
    fi
done

if [ "${TOTAL_TESTS}" -eq 0 ]; then
    echo -e "${RED}No valid integration test files found${NC}"
    exit 1
fi

# Print summary
echo -e "${YELLOW}=== Test Summary ===${NC}"
echo -e "Total: ${TOTAL_TESTS}"
echo -e "${GREEN}Passed: ${PASSED_TESTS}${NC}"
if [ "${FAILED_TESTS}" -gt 0 ]; then
    echo -e "${RED}Failed: ${FAILED_TESTS}${NC}"
    echo -e "${RED}Server log (last 50 lines):${NC}"
    tail -n 50 $SERVER_LOG 2>/dev/null || echo "No server log found"
    exit 1
else
    echo -e "${GREEN}✓ All integration tests passed!${NC}"
    exit 0
fi
