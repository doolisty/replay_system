/**
 * Multiprocess solution - Recorder process
 * Consumes messages from shared memory and persists to disk
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <optional>
#include <sstream>
#include <thread>
#include <vector>

#include "channel/FileChannel.hpp"
#include "common/CpuAffinity.hpp"
#include "common/Logging.hpp"
#include "common/Message.hpp"
#include "common/Types.hpp"

namespace {

// Shared memory name (consistent with server)
const char* SHM_NAME = "/mktdata_rb";

// Shared memory size
constexpr size_t SHM_RING_BUFFER_SIZE = 1024 * 64;
constexpr size_t CACHE_LINE_SIZE = 64;

// Batch write size
constexpr size_t BATCH_SIZE = 1024;

// Shared ring buffer structure (consistent with server)
struct alignas(CACHE_LINE_SIZE) SharedSlot {
  replay::Msg msg;
  std::atomic<replay::SeqNum> seq;
  char padding[CACHE_LINE_SIZE - sizeof(replay::Msg) -
               sizeof(std::atomic<replay::SeqNum>)];
};

struct SharedRingBuffer {
  alignas(CACHE_LINE_SIZE) std::atomic<replay::SeqNum> write_seq;
  alignas(CACHE_LINE_SIZE) std::atomic<bool> server_running;
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> total_messages;

  SharedSlot slots[SHM_RING_BUFFER_SIZE];

  std::optional<replay::Msg> read(replay::SeqNum expected_seq) const {
    if (expected_seq < 0) {
      return std::nullopt;
    }

    size_t index = expected_seq % SHM_RING_BUFFER_SIZE;
    const SharedSlot& slot = slots[index];

    replay::SeqNum published_seq = slot.seq.load(std::memory_order_acquire);

    if (published_seq == expected_seq) {
      return slot.msg;
    }

    return std::nullopt;
  }

  replay::SeqNum getLatestSeq() const {
    return write_seq.load(std::memory_order_acquire) - 1;
  }

  bool isServerRunning() const {
    return server_running.load(std::memory_order_acquire);
  }
};

// Global variables
SharedRingBuffer* g_buffer = nullptr;
std::atomic<bool> g_stop_requested{false};

int g_shm_fd = -1;

// Signal handler
void signalHandler(int sig) {
  std::cout << "\nReceived signal " << sig << ", stopping..." << std::endl;
  g_stop_requested = true;
}

// Connect to shared memory
bool connectToSharedMemory() {
  g_shm_fd = shm_open(SHM_NAME, O_RDWR, 0666);
  if (g_shm_fd == -1) {
    return false;
  }

  g_buffer = static_cast<SharedRingBuffer*>(
      mmap(nullptr, sizeof(SharedRingBuffer), PROT_READ | PROT_WRITE,
           MAP_SHARED, g_shm_fd, 0));

  if (g_buffer == MAP_FAILED) {
    close(g_shm_fd);
    return false;
  }

  return true;
}

// Disconnect from shared memory
void disconnectFromSharedMemory() {
  if (g_buffer && g_buffer != MAP_FAILED) {
    munmap(g_buffer, sizeof(SharedRingBuffer));
    g_buffer = nullptr;
  }
  if (g_shm_fd != -1) {
    close(g_shm_fd);
    g_shm_fd = -1;
  }
}

// Get current date string
std::string getDateString() {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm = *std::localtime(&time);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y%m%d");
  return oss.str();
}

}  // namespace

int main(int argc, char* argv[]) {
  auto* logger = replay::initLogger("ipc_recorder");
  std::cout << "=== Multiprocess Recorder ===" << std::endl;
  LOG_INFO(logger, "ipc_recorder start {}", "");

  // Default output file
  std::string output_file = "data/mktdata_ipc_" + getDateString() + ".bin";

  // Parse command line arguments
  int cpu_core = replay::CPU_CORE_UNSET;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.find("--output=") == 0) {
      output_file = arg.substr(9);
    } else if (arg.find("--cpu=") == 0) {
      cpu_core = std::stoi(arg.substr(6));
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]\n"
                << "  --output=<file>  Output file path (default: "
                   "data/mktdata_ipc_YYYYMMDD.bin)\n"
                << "  --cpu=<core>     Pin process to CPU core\n"
                << std::endl;
      return 0;
    }
  }

  // Set CPU affinity for this process
  replay::setCpuAffinity(cpu_core, "ipc_recorder");

  std::cout << "Output file: " << output_file << std::endl;
  LOG_INFO(logger, "Output file: {}", output_file);

  // Set signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  // Try to connect to shared memory
  int retry_count = 0;
  const int max_retries = 30;

  while (!connectToSharedMemory() && retry_count < max_retries) {
    std::cout << "Waiting for server to start... (" << retry_count + 1 << "/"
              << max_retries << ")" << std::endl;
    LOG_INFO(logger, "Waiting for server to start: attempt {}/{}",
             retry_count + 1, max_retries);
    std::this_thread::sleep_for(std::chrono::seconds(1));
    retry_count++;
  }

  if (g_buffer == nullptr) {
    std::cerr << "Cannot connect to shared memory, please start server first"
              << std::endl;
    LOG_ERROR(logger, "Cannot connect to shared memory, server not running");
    return 1;
  }

  std::cout << "Connected to shared memory" << std::endl;

  // Create file write channel
  replay::FileWriteChannel channel(output_file);
  if (!channel.open()) {
    std::cerr << "Cannot create output file: " << output_file << std::endl;
    LOG_ERROR(logger, "Cannot create output file: {}", output_file);
    disconnectFromSharedMemory();
    return 1;
  }

  // Consume and record messages
  replay::SeqNum read_seq = 0;
  int64_t recorded_count = 0;
  double expected_sum = 0.0;
  double kahan_c = 0.0;

  std::vector<replay::Msg> batch;
  batch.reserve(BATCH_SIZE);

  auto start_time = std::chrono::high_resolution_clock::now();

  while (!g_stop_requested) {
    auto msg = g_buffer->read(read_seq);

    if (msg) {
      batch.push_back(*msg);

      // Kahan summation
      double y = msg->payload - kahan_c;
      double t = expected_sum + y;
      kahan_c = (t - expected_sum) - y;
      expected_sum = t;

      recorded_count++;
      read_seq++;

      // Batch write
      if (batch.size() >= BATCH_SIZE) {
        for (const auto& m : batch) {
          channel.write(m);
        }
        batch.clear();
        channel.flush();
      }

      // Progress display
      if (recorded_count % 10000 == 0) {
        std::cout << "Recorded: " << recorded_count << " messages" << std::endl;
      }
    } else {
      // Write remaining data
      if (!batch.empty()) {
        for (const auto& m : batch) {
          channel.write(m);
        }
        batch.clear();
        channel.flush();
      }

      // Check if server is still running
      if (!g_buffer->isServerRunning()) {
        replay::SeqNum latest = g_buffer->getLatestSeq();
        if (read_seq > latest) {
          break;
        }
      }
      std::this_thread::yield();
    }
  }

  // Write remaining data
  if (!batch.empty()) {
    for (const auto& m : batch) {
      channel.write(m);
    }
    channel.flush();
  }

  channel.close();

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  // Print results
  std::cout << "\n=== Recorder Results ===" << std::endl;
  std::cout << "Recorded messages: " << recorded_count << " messages"
            << std::endl;
  std::cout << "Expected sum: " << std::fixed << expected_sum << std::endl;
  std::cout << "Output file: " << output_file << std::endl;
  std::cout << "Time: " << duration.count() << " ms" << std::endl;

  LOG_INFO(
      logger,
      "ipc_recorder complete: recorded={}, expected_sum={}, duration_ms={}",
      recorded_count, expected_sum, duration.count());

  if (recorded_count > 0) {
    double throughput =
        static_cast<double>(recorded_count) * 1000.0 / duration.count();
    std::cout << "Throughput: " << static_cast<int64_t>(throughput) << " msg/s"
              << std::endl;
  }

  // Cleanup
  disconnectFromSharedMemory();

  std::cout << "\nUse the following command to verify results:" << std::endl;
  std::cout << "  python scripts/verify_result.py " << output_file << std::endl;

  return 0;
}
