/**
 * Multiprocess solution - Client process
 * Consumes messages from shared memory and accumulates payload
 */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <optional>
#include <thread>

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
    std::cerr << "shm_open failed: " << strerror(errno) << std::endl;
    LOG_ERROR(replay::logger(), "shm_open failed: {}", strerror(errno));
    return false;
  }

  g_buffer = static_cast<SharedRingBuffer*>(
      mmap(nullptr, sizeof(SharedRingBuffer), PROT_READ | PROT_WRITE,
           MAP_SHARED, g_shm_fd, 0));

  if (g_buffer == MAP_FAILED) {
    std::cerr << "mmap failed: " << strerror(errno) << std::endl;
    LOG_ERROR(replay::logger(), "mmap failed: {}", strerror(errno));
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

}  // namespace

int main(int argc, char* argv[]) {
  auto* logger = replay::initLogger("ipc_client");
  std::cout << "=== Multiprocess Client ===" << std::endl;
  LOG_INFO(logger, "ipc_client start {}", "");

  // Parse command line arguments
  int cpu_core = replay::CPU_CORE_UNSET;
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.find("--cpu=") == 0) {
      cpu_core = std::stoi(arg.substr(6));
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]\n"
                << "  --cpu=<core>  Pin process to CPU core\n"
                << std::endl;
      return 0;
    }
  }

  // Set CPU affinity for this process
  replay::setCpuAffinity(cpu_core, "ipc_client");

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
  LOG_INFO(logger, "Connected to shared memory {}", "");

  // Consume messages
  replay::SeqNum read_seq = 0;
  int64_t processed_count = 0;
  double sum = 0.0;
  double kahan_c = 0.0;  // Kahan summation compensation

  auto start_time = std::chrono::high_resolution_clock::now();

  while (!g_stop_requested) {
    auto msg = g_buffer->read(read_seq);

    if (msg) {
      // Kahan summation
      double y = msg->payload - kahan_c;
      double t = sum + y;
      kahan_c = (t - sum) - y;
      sum = t;

      processed_count++;
      read_seq++;

      // Progress display
      if (processed_count % 10000 == 0) {
        std::cout << "Processed: " << processed_count
                  << " messages, current sum: " << sum << std::endl;
      }
    } else {
      // Check if server is still running
      if (!g_buffer->isServerRunning()) {
        // Server has stopped, try to process remaining messages
        replay::SeqNum latest = g_buffer->getLatestSeq();
        if (read_seq > latest) {
          break;  // All messages processed
        }
      }
      std::this_thread::yield();
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  // Print results
  std::cout << "\n=== Client Results ===" << std::endl;
  std::cout << "Processed messages: " << processed_count << " messages"
            << std::endl;
  std::cout << "Sum: " << std::fixed << sum << std::endl;
  std::cout << "Last sequence number: " << read_seq - 1 << std::endl;
  std::cout << "Time: " << duration.count() << " ms" << std::endl;

  LOG_INFO(
      logger,
      "ipc_client complete: processed={}, sum={}, last_seq={}, duration_ms={}",
      processed_count, sum, read_seq - 1, duration.count());

  if (processed_count > 0) {
    double throughput =
        static_cast<double>(processed_count) * 1000.0 / duration.count();
    std::cout << "Throughput: " << static_cast<int64_t>(throughput) << " msg/s"
              << std::endl;
  }

  // Cleanup
  disconnectFromSharedMemory();

  return 0;
}
