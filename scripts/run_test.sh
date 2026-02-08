#!/bin/bash
# Test run script

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${GREEN}=== ReplaySystem Test Script ===${NC}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
BUILD_DIR="$PROJECT_DIR/build"

# Check if compiled
if [[ ! -d "$BUILD_DIR" ]]; then
    echo -e "${RED}Error: Please run build.sh to compile the project first${NC}"
    exit 1
fi

cd "$BUILD_DIR"

# Create data directories (project data + build/data for test binaries that use relative path "data/")
mkdir -p "$PROJECT_DIR/data"
mkdir -p "$BUILD_DIR/data"

# Default test type
TEST_TYPE="all"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --basic)
            TEST_TYPE="basic"
            shift
            ;;
        --recovery)
            TEST_TYPE="recovery"
            shift
            ;;
        --consistency)
            TEST_TYPE="consistency"
            shift
            ;;
        --stress)
            TEST_TYPE="stress"
            shift
            ;;
        --benchmark)
            TEST_TYPE="benchmark"
            shift
            ;;
        --all)
            TEST_TYPE="all"
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --basic        Basic functionality test"
            echo "  --recovery     Fault recovery test"
            echo "  --consistency  Consistency test"
            echo "  --stress       Stress test"
            echo "  --benchmark    Benchmark test"
            echo "  --all          Run all tests (default)"
            echo "  --help         Show help information"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown parameter: $1${NC}"
            exit 1
            ;;
    esac
done

run_basic_test() {
    echo -e "${YELLOW}--- Basic Functionality Test ---${NC}"
    ./replay_system --mode=test --messages=10000 --rate=5000 --data-dir="$PROJECT_DIR/data"
    echo ""
}

run_recovery_test() {
    echo -e "${YELLOW}--- Fault Recovery Test ---${NC}"
    if [[ -f "./test_recovery" ]]; then
        ./test_recovery
    else
        ./replay_system --mode=recovery_test --messages=5000 --fault-at=2500 --data-dir="$PROJECT_DIR/data"
    fi
    echo ""
}

run_consistency_test() {
    echo -e "${YELLOW}--- Consistency Test ---${NC}"
    if [[ -f "./test_consistency" ]]; then
        ./test_consistency
    else
        echo "Consistency test requires compiling test_consistency"
    fi
    echo ""
}

run_stress_test() {
    echo -e "${YELLOW}--- Stress Test ---${NC}"
    ./replay_system --mode=stress --messages=100000 --rate=50000 --data-dir="$PROJECT_DIR/data"
    echo ""
}

run_benchmark_test() {
    echo -e "${YELLOW}--- Benchmark Test ---${NC}"
    if [[ -f "./test_benchmark" ]]; then
        ./test_benchmark
    else
        echo "Benchmark test requires compiling test_benchmark"
    fi
    echo ""
}

# Run tests
case $TEST_TYPE in
    basic)
        run_basic_test
        ;;
    recovery)
        run_recovery_test
        ;;
    consistency)
        run_consistency_test
        ;;
    stress)
        run_stress_test
        ;;
    benchmark)
        run_benchmark_test
        ;;
    all)
        run_basic_test
        run_recovery_test
        run_consistency_test
        run_stress_test
        run_benchmark_test
        ;;
esac

echo -e "${GREEN}Tests completed!${NC}"

# Verify results
if [[ -f "$SCRIPT_DIR/verify_result.py" ]]; then
    echo ""
    echo -e "${YELLOW}Running verification script...${NC}"
    python3 "$SCRIPT_DIR/verify_result.py" "$PROJECT_DIR/data/"*.bin 2>/dev/null || true
fi
