# ReplaySystem - Cross-platform build and run
# Ubuntu 22.04 + GCC 11 (C++20)
ARG BASE_IMAGE=ubuntu:22.04
FROM ${BASE_IMAGE}

ENV DEBIAN_FRONTEND=noninteractive

# Build and runtime dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    cmake \
    git \
    python3 \
    python3-setuptools \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy project (including .git for submodule init)
COPY . .

# # Init quill submodule (no-op if already present)
# RUN if [ -d .git ]; then git submodule update --init --recursive; fi

# Build project (Release, with tests)
RUN mkdir -p build && cd build \
    && cmake .. \
        -DCMAKE_BUILD_TYPE=Release \
        -DBUILD_TESTS=ON \
        -DBUILD_MULTIPROCESS=OFF \
    && cmake --build . -j$(nproc)

# Data directory (mount or write at runtime)
RUN mkdir -p /app/data

# Default: run all tests
CMD ["/bin/bash", "-c", "cd /app/build && /bin/bash /app/scripts/run_test.sh --all"]
