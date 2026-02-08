/**
 * Multiprocess solution - Server process
 * Uses shared memory for inter-process communication
 */

#include <fcntl.h>
#include <pthread.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstring>
#include <iostream>
#include <random>
#include <thread>

#include "common/CpuAffinity.hpp"
#include "common/Logging.hpp"
#include "common/Message.hpp"
#include "common/Types.hpp"

namespace {

// Shared memory name
const char* SHM_NAME = "/mktdata_rb";

// Shared memory size
constexpr size_t SHM_RING_BUFFER_SIZE = 1024 * 64;  // 64K entries
constexpr size_t CACHE_LINE_SIZE = 64;

// Shared ring buffer structure
struct alignas(CACHE_LINE_SIZE) SharedSlot {
  replay::Msg msg;
  std::atomic<replay::SeqNum> seq;
  char padding[CACHE_LINE_SIZE - sizeof(replay::Msg) -
               sizeof(std::atomic<replay::SeqNum>)];
};

struct SharedRingBuffer {
  // Control information
  alignas(CACHE_LINE_SIZE) std::atomic<replay::SeqNum> write_seq;
  alignas(CACHE_LINE_SIZE) std::atomic<bool> server_running;
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> total_messages;

  // Data slots
  SharedSlot slots[SHM_RING_BUFFER_SIZE];

  void init() {
    write_seq.store(0, std::memory_order_relaxed);
    server_running.store(true, std::memory_order_relaxed);
    total_messages.store(0, std::memory_order_relaxed);

    for (auto& slot : slots) {
      slot.seq.store(replay::INVALID_SEQ, std::memory_order_relaxed);
    }
  }

  replay::SeqNum push(const replay::Msg& msg) {
    replay::SeqNum seq = write_seq.fetch_add(1, std::memory_order_relaxed);
    size_t index = seq % SHM_RING_BUFFER_SIZE;

    SharedSlot& slot = slots[index];
    slot.msg = msg;
    slot.msg.seq_num = seq;
    slot.seq.store(seq, std::memory_order_release);

    return seq;
  }

  replay::SeqNum getLatestSeq() const {
    return write_seq.load(std::memory_order_acquire) - 1;
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
  if (g_buffer) {
    g_buffer->server_running.store(false, std::memory_order_release);
  }
}

// Create shared memory
bool createSharedMemory() {
  // First try to remove existing shared memory
  shm_unlink(SHM_NAME);

  g_shm_fd = shm_open(SHM_NAME, O_CREAT | O_RDWR, 0666);
  if (g_shm_fd == -1) {
    std::cerr << "shm_open failed: " << strerror(errno) << std::endl;
    LOG_ERROR(replay::logger(), "shm_open failed: {}", strerror(errno));
    return false;
  }

  if (ftruncate(g_shm_fd, sizeof(SharedRingBuffer)) == -1) {
    std::cerr << "ftruncate failed: " << strerror(errno) << std::endl;
    LOG_ERROR(replay::logger(), "ftruncate failed: {}", strerror(errno));
    close(g_shm_fd);
    shm_unlink(SHM_NAME);
    return false;
  }

  g_buffer = static_cast<SharedRingBuffer*>(
      mmap(nullptr, sizeof(SharedRingBuffer), PROT_READ | PROT_WRITE,
           MAP_SHARED, g_shm_fd, 0));

  if (g_buffer == MAP_FAILED) {
    std::cerr << "mmap failed: " << strerror(errno) << std::endl;
    LOG_ERROR(replay::logger(), "mmap failed: {}", strerror(errno));
    close(g_shm_fd);
    shm_unlink(SHM_NAME);
    return false;
  }

  // Initialize shared memory
  g_buffer->init();

  return true;
}

// Cleanup shared memory
void cleanupSharedMemory() {
  if (g_buffer) {
    g_buffer->server_running.store(false, std::memory_order_release);
  }

  if (g_buffer && g_buffer != MAP_FAILED) {
    munmap(g_buffer, sizeof(SharedRingBuffer));
    g_buffer = nullptr;
  }
  if (g_shm_fd != -1) {
    close(g_shm_fd);
    g_shm_fd = -1;
  }
  shm_unlink(SHM_NAME);
}

}  // namespace

int main(int argc, char* argv[]) {
  auto* logger = replay::initLogger("ipc_server");
  std::cout << "=== Multiprocess Server ===" << std::endl;

  // Default parameters
  int64_t message_count = 10000;
  int64_t message_rate = 1000;
  int cpu_core = replay::CPU_CORE_UNSET;

  // Parse command line arguments
  for (int i = 1; i < argc; ++i) {
    std::string arg = argv[i];
    if (arg.find("--messages=") == 0) {
      message_count = std::stoll(arg.substr(11));
    } else if (arg.find("--rate=") == 0) {
      message_rate = std::stoll(arg.substr(7));
    } else if (arg.find("--cpu=") == 0) {
      cpu_core = std::stoi(arg.substr(6));
    } else if (arg == "--help") {
      std::cout << "Usage: " << argv[0] << " [options]\n"
                << "  --messages=<count>  Message count (default: 10000)\n"
                << "  --rate=<rate>       Messages per second (default: 1000)\n"
                << "  --cpu=<core>        Pin process to CPU core\n"
                << std::endl;
      return 0;
    }
  }

  // Set CPU affinity for this process
  replay::setCpuAffinity(cpu_core, "ipc_server");

  std::cout << "Message count: " << message_count << std::endl;
  std::cout << "Send rate: " << message_rate << "/s" << std::endl;
  std::cout << "Shared memory size: " << sizeof(SharedRingBuffer) / 1024 / 1024
            << " MB" << std::endl;

  LOG_INFO(logger, "ipc_server start: messages={}, rate={}, shm_mb={}",
           message_count, message_rate, sizeof(SharedRingBuffer) / 1024 / 1024);

  // Set signal handler
  std::signal(SIGINT, signalHandler);
  std::signal(SIGTERM, signalHandler);

  // Create shared memory
  if (!createSharedMemory()) {
    std::cerr << "Failed to create shared memory" << std::endl;
    LOG_ERROR(logger, "Failed to create shared memory {}", "");
    return 1;
  }

  std::cout << "Shared memory created, waiting for client connection..."
            << std::endl;
  LOG_INFO(logger, "Shared memory created {}", "");

  // Random number generator
  std::mt19937 rng(std::random_device{}());
  std::uniform_real_distribution<double> dist(0.0, 100.0);

  // Calculate interval time
  auto interval_ns = std::chrono::nanoseconds(1000000000 / message_rate);
  auto start_time = std::chrono::high_resolution_clock::now();

  // Send messages
  double total_payload = 0.0;

  for (int64_t i = 0; i < message_count && !g_stop_requested; ++i) {
    double payload = dist(rng);
    int64_t timestamp = replay::getCurrentTimestampNs();

    replay::Msg msg(replay::INVALID_SEQ, timestamp, payload);
    g_buffer->push(msg);
    g_buffer->total_messages.fetch_add(1, std::memory_order_release);

    total_payload += payload;

    // Rate control
    if (message_rate > 0) {
      auto expected_time = start_time + interval_ns * (i + 1);
      auto now = std::chrono::high_resolution_clock::now();
      if (now < expected_time) {
        std::this_thread::sleep_until(expected_time);
      }
    }

    // Progress display
    if ((i + 1) % (message_count / 10) == 0) {
      std::cout << "Progress: " << (i + 1) * 100 / message_count << "%"
                << std::endl;
    }
  }

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  std::cout << "\n=== Server Complete ===" << std::endl;
  std::cout << "Sent messages: " << g_buffer->total_messages.load()
            << " messages" << std::endl;
  std::cout << "Sum: " << std::fixed << total_payload << std::endl;
  std::cout << "Time: " << duration.count() << " ms" << std::endl;

  LOG_INFO(logger, "ipc_server complete: sent={}, sum={}, duration_ms={}",
           g_buffer->total_messages.load(), total_payload, duration.count());

  // Wait for clients to finish processing
  std::cout << "Waiting for clients to process..." << std::endl;
  std::this_thread::sleep_for(std::chrono::seconds(2));

  // Cleanup
  cleanupSharedMemory();
  std::cout << "Shared memory cleaned up" << std::endl;
  LOG_INFO(logger, "Shared memory cleaned up {}", "");

  return 0;
}
