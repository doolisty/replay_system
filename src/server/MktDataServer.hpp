#pragma once

#include <atomic>
#include <functional>
#include <random>
#include <thread>

#include "common/CpuAffinity.hpp"
#include "common/RingBuffer.hpp"

namespace replay {

// Market data server
// Independent thread generates simulated market data and writes to RingBuffer
class MktDataServer {
 public:
  using RingBufferType = RingBuffer<DEFAULT_RING_BUFFER_SIZE>;
  using MessageGenerator = std::function<double()>;

  explicit MktDataServer(RingBufferType& buffer);
  ~MktDataServer();

  // Disable copy and move
  MktDataServer(const MktDataServer&) = delete;
  MktDataServer& operator=(const MktDataServer&) = delete;
  MktDataServer(MktDataServer&&) = delete;
  MktDataServer& operator=(MktDataServer&&) = delete;

  // Start service
  void start();

  // Stop service
  void stop();

  // Wait for service to complete
  void waitForComplete();

  // Check if running
  bool isRunning() const;

  // Set message generation parameters
  void setMessageCount(int64_t count);
  void setMessageRate(int64_t rate_per_second);

  // Set custom message generator
  void setMessageGenerator(MessageGenerator generator);

  // Set CPU core for this thread (call before start())
  void setCpuCore(int core_id);

  // Get count of sent messages
  int64_t getSentCount() const;

  // Get latest sent sequence number
  SeqNum getLatestSeq() const;

 private:
  void run();
  double generatePayload();

  RingBufferType& buffer_;
  std::thread thread_;
  std::atomic<bool> running_;
  std::atomic<bool> stop_requested_;

  int64_t message_count_;
  int64_t message_rate_;
  std::atomic<int64_t> sent_count_;

  MessageGenerator generator_;
  std::mt19937 rng_;
  std::uniform_real_distribution<double> dist_;

  int cpu_core_ = CPU_CORE_UNSET;
};

}  // namespace replay
