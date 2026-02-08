#!/bin/bash
# Build script

set -e

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${GREEN}=== ReplaySystem Build Script ===${NC}"

# Get script directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Default values
BUILD_TYPE="Release"
BUILD_TESTS="ON"
BUILD_MULTIPROCESS="OFF"
CLEAN_BUILD="OFF"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --debug)
            BUILD_TYPE="Debug"
            shift
            ;;
        --release)
            BUILD_TYPE="Release"
            shift
            ;;
        --no-tests)
            BUILD_TESTS="OFF"
            shift
            ;;
        --multiprocess)
            BUILD_MULTIPROCESS="ON"
            shift
            ;;
        --clean)
            CLEAN_BUILD="ON"
            shift
            ;;
        --help)
            echo "Usage: $0 [options]"
            echo "Options:"
            echo "  --debug         Build in Debug mode"
            echo "  --release       Build in Release mode (default)"
            echo "  --no-tests      Do not build tests"
            echo "  --multiprocess  Build multiprocess solution"
            echo "  --clean         Clean and rebuild"
            echo "  --help          Show help information"
            exit 0
            ;;
        *)
            echo -e "${RED}Unknown parameter: $1${NC}"
            exit 1
            ;;
    esac
done

cd "$PROJECT_DIR"

# Clean build directory
if [[ "$CLEAN_BUILD" == "ON" ]]; then
    echo -e "${YELLOW}Cleaning build directory...${NC}"
    rm -rf build
fi

# Create build directory
mkdir -p build
cd build

# Configure CMake
echo -e "${GREEN}Configuring CMake...${NC}"
echo "  BUILD_TYPE: $BUILD_TYPE"
echo "  BUILD_TESTS: $BUILD_TESTS"
echo "  BUILD_MULTIPROCESS: $BUILD_MULTIPROCESS"

cmake .. \
    -DCMAKE_BUILD_TYPE="$BUILD_TYPE" \
    -DBUILD_TESTS="$BUILD_TESTS" \
    -DBUILD_MULTIPROCESS="$BUILD_MULTIPROCESS"

# 将 compile_commands.json 链接到项目根目录，便于 clangd 识别 C++ 标准与 include
if [[ -f compile_commands.json ]]; then
    ln -sf "$(pwd)/compile_commands.json" "$PROJECT_DIR/compile_commands.json"
fi

# Get CPU core count
if [[ "$(uname)" == "Darwin" ]]; then
    NPROC=$(sysctl -n hw.ncpu)
else
    NPROC=$(nproc)
fi

# Build
echo -e "${GREEN}Building (using $NPROC cores)...${NC}"
cmake --build . -j"$NPROC"

echo -e "${GREEN}Build completed!${NC}"
echo ""
echo "Executables located at: $PROJECT_DIR/build/"
    echo "  - replay_system      Main program"
if [[ "$BUILD_TESTS" == "ON" ]]; then
    echo "  - test_recovery      Fault recovery test"
    echo "  - test_consistency   Consistency test"
fi
if [[ "$BUILD_MULTIPROCESS" == "ON" ]]; then
    echo "  - ipc_server         Multiprocess server"
    echo "  - ipc_client         Multiprocess client"
    echo "  - ipc_recorder       Multiprocess recorder"
fi
