# Realtime Data Replay System (ReplaySystem)

A high-performance real-time market data replay system with recording, fault recovery, and seamless catch-up switching.

## Features

- **High-performance messaging**: Lock-free SPMC ring buffer
- **Persistence**: Efficient disk recording and replay
- **Fault recovery**: Automatic recovery after client crash
- **Catch-up switching**: Seamless switch from history to live data stream

## Architecture

```
┌─────────────┐     ┌──────────────────┐     ┌─────────────┐
│ MktDataServer│────▶│  RingBuffer      │────▶│MktDataClient│
│  (Producer)  │     │ (Lock-free SPMC) │     │ (Consumer)  │
└─────────────┘     └──────────────────┘     └─────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │ MktDataRecorder  │
                    │  (Persistence)   │
                    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │   Disk File      │
                    │ (mktdata_*.bin)  │
                    └──────────────────┘
                            │
                            ▼
                    ┌──────────────────┐
                    │  ReplayEngine    │
                    │ (Recovery/Replay)│
                    └──────────────────┘
```

## Dependencies

- C++20 or later
- CMake >= 3.14
- Thread library (pthread / Windows Threads)
- Optional: Google Test (unit tests)

## Docker (cross-platform build / run / test)

No need to install CMake, a compiler, or dependencies on the host; use Docker to build, run, and test on any platform.

**Requirements**: [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) installed.

```bash
# If Docker Hub is blocked, use a mirror base image:
export BASE_IMAGE=docker.xuanyuan.me/library/ubuntu:22.04

# Build image and run all tests (default)
docker compose up --build

# Build image only
docker compose build

# Run main program (e.g. basic test with 10000 messages)
docker compose run --rm app /app/build/replay_system --mode=test --messages=10000 --data-dir=/app/data

# Run basic tests only
docker compose run --rm app /bin/bash -c "cd /app/build && /app/scripts/run_test.sh --basic"

# Shell into container for debugging
docker compose run --rm app /bin/bash
```

Data directory is persisted via volume `replay_data`. To inspect recording files on the host, mount a local directory:

```yaml
# In docker-compose.yml under app.volumes, use:
# - ./data:/app/data
```

## Build

### Linux/macOS

```bash
# Create build directory
mkdir -p build && cd build

# Configure CMake (Release)
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build . -j$(nproc)
```

### Windows (MSVC)

```powershell
# Create build directory
mkdir build
cd build

# Configure CMake
cmake .. -DCMAKE_BUILD_TYPE=Release

# Build
cmake --build . --config Release
```

### Build options

| Option | Default | Description |
|--------|---------|-------------|
| `BUILD_TESTS` | ON | Build unit tests |
| `BUILD_MULTIPROCESS` | OFF | Build multi-process solution |

```bash
# Enable multi-process build
cmake .. -DBUILD_MULTIPROCESS=ON
```

## Run

### Basic test

```bash
./replay_system --mode=test --messages=10000 --rate=1000
```

### CPU affinity (optional)

Pin threads to specific CPU cores using a comma-separated list.
Order: main, server, client, recorder. Unspecified threads are not pinned.

```bash
# Pin all threads
./replay_system --mode=test --messages=10000 --rate=1000 --cpu=0,1,2,3

# Pin main and server only
./replay_system --mode=test --messages=10000 --rate=1000 --cpu=0,1
```

### Fault recovery test

```bash
./replay_system --mode=recovery_test --fault-at=5000
```

### Stress test

```bash
./replay_system --mode=stress --messages=1000000 --rate=100000
```

### Performance benchmarks

Run the benchmark suite (latency and throughput of RingBuffer, file I/O, ReplayEngine, full pipeline). See [Performance benchmarks](#performance-benchmarks) for details.

```bash
cd build && ./test_benchmark
```

### Verify results

```bash
python3 scripts/verify_result.py data/mktdata_*.bin
```

## Project structure

```
ReplaySystem/
├── CMakeLists.txt              # CMake build config
├── README.md                   # Project readme
├── src/
│   ├── main.cpp                # Entry point
│   ├── common/                 # Common components
│   │   ├── Message.hpp         # Message struct
│   │   ├── RingBuffer.hpp      # Lock-free ring buffer
│   │   ├── SpinLock.hpp        # Spinlock
│   │   ├── CpuAffinity.hpp     # CPU affinity helper (Linux)
│   │   └── Types.hpp           # Common types
│   ├── server/                 # Server
│   │   ├── MktDataServer.hpp
│   │   └── MktDataServer.cpp
│   ├── client/                 # Client
│   │   ├── MktDataClient.hpp
│   │   └── MktDataClient.cpp
│   ├── recorder/               # Recorder
│   │   ├── MktDataRecorder.hpp
│   │   └── MktDataRecorder.cpp
│   ├── replay/                 # Replay engine
│   │   ├── ReplayEngine.hpp
│   │   └── ReplayEngine.cpp
│   └── channel/                # Channel abstraction
│       ├── IChannel.hpp
│       ├── SharedMemChannel.hpp
│       └── FileChannel.hpp
├── test/                       # Tests
├── scripts/                    # Scripts
├── data/                       # Runtime data
└── multiprocess/               # Multi-process solution
```

## Message format

### Message layout (24 bytes)

| Field | Type | Size | Description |
|-------|------|------|-------------|
| seq_num | int64_t | 8B | Sequence number |
| timestamp_ns | int64_t | 8B | Timestamp (nanoseconds) |
| payload | double | 8B | Payload |

### Disk file format

```
File header (32 bytes):
┌─────────────────────────────────────────────────────┐
│ magic (4B) │ version (2B) │ date (4B) │ reserved   │
└─────────────────────────────────────────────────────┘

Message records (24 bytes each):
┌─────────────────────────────────────────────────────┐
│ seq_num (8B) │ timestamp_ns (8B) │ payload (8B)    │
└─────────────────────────────────────────────────────┘
```

## Fault recovery flow

1. Client detects fault (e.g. running sum reset).
2. ReplayEngine starts and reads history from disk.
3. Replays messages one by one to rebuild state.
4. When replay sequence nears live sequence, switches seamlessly to live feed.
5. Continues consuming live data as normal.

## Performance targets

| Metric | Target |
|--------|--------|
| Message latency | < 1μs |
| Disk write throughput | > 100MB/s |
| Replay throughput | > 1M msg/s |

## Performance benchmarks

The benchmark suite (`test/test_benchmark.cpp`) measures latency and throughput of core components and the full pipeline. Use it to validate performance targets and track regressions.

### What it measures

| Benchmark | Description |
|-----------|-------------|
| **RingBuffer Push Latency** | Per-message write cost (single producer). Target: median &lt; 500 ns. |
| **RingBuffer Read Latency** | Per-message read cost (`readEx`). Target: median &lt; 500 ns. |
| **RingBuffer SPSC Throughput** | One producer, one consumer; max msg/s. Target: &gt; 1M msg/s. |
| **RingBuffer SPMC Throughput** | One producer, four consumers. Target: &gt; 0.5M msg/s. |
| **RingBuffer Batch Push** | Single push vs batch push (batch size 64); reports msg/s for both. |
| **File Write Throughput** | Sequential write via `FileWriteChannel`. Target: &gt; 50 MB/s. |
| **File Read Throughput** | Sequential read via `FileChannel`. Target: &gt; 50 MB/s. |
| **ReplayEngine Throughput** | Replay from disk including sequence validation. Target: &gt; 1M msg/s. |
| **SpinLock Contention** | Lock/unlock throughput with 1, 2, and 4 threads. |
| **End-to-End Latency** | Producer push → consumer read (timestamp delta). Target: median &lt; 10 μs. |
| **Full System Throughput** | Server + Client + Recorder; correctness (sum match) and msg/s. Target: &gt; 100K msg/s. |
| **Recovery Latency** | Wall-clock time from fault injection to recovery completion. Target: &lt; 5 s. |

Benchmarks print statistics (min/mean/median/p90/p99/max for latency, throughput in msg/s or MB/s) and assert against the targets above so the suite can double as a performance regression test.

### Build and run

Build with tests enabled (default):

```bash
cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build . -j$(nproc)
```

Run all benchmarks:

```bash
./test_benchmark
```

With CTest (when Google Test is found):

```bash
ctest -R BenchmarkTest -V
```

Run only the benchmark executable (no other tests):

```bash
./test_benchmark
```

Benchmark data files are written under `data/` (e.g. `data/bench_file_write.bin`). The suite does not clean them up; you may remove `data/bench_*.bin` after a run if desired.

## License

MIT License
