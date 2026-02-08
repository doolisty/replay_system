#pragma once

#include <atomic>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

#include "common/CpuAffinity.hpp"
#include "common/Message.hpp"
#include "common/RingBuffer.hpp"
#include "common/Types.hpp"

namespace replay {

// Forward declaration
class ReplayEngine;

// Observability metrics for the client — all atomics for thread-safe reads
struct ClientMetrics {
  std::atomic<int64_t> seq_gap_count{0};      // Sequence gaps detected (overwrite or skip)
  std::atomic<int64_t> overwrite_count{0};     // Ring buffer overwrites detected
  std::atomic<int64_t> recovery_count{0};      // Number of recovery cycles
  std::atomic<int64_t> auto_fault_count{0};    // Auto-detected faults
};

// Market data client
// Independent thread consumes messages, accumulates payload, supports fault
// recovery
//
// Correctness invariants:
//   INV-C1: processMessage() is called with strictly increasing seq_num
//           (no duplicates, no out-of-order) within each processing epoch.
//   INV-C2: On replay-to-live switch, first_live_seq == last_replay_seq + 1
//           (no gap, no overlap at the boundary).
//   INV-C3: After successful recovery, the accumulated sum equals what a
//           fault-free client would have computed.
class MktDataClient {
 public:
  using RingBufferType = RingBuffer<DEFAULT_RING_BUFFER_SIZE>;
  using FaultCallback = std::function<void()>;

  MktDataClient(RingBufferType& buffer, const std::string& disk_file);
  ~MktDataClient();

  // Disable copy and move
  MktDataClient(const MktDataClient&) = delete;
  MktDataClient& operator=(const MktDataClient&) = delete;
  MktDataClient(MktDataClient&&) = delete;
  MktDataClient& operator=(MktDataClient&&) = delete;

  // Start client
  void start();

  // Stop client
  void stop();

  // Wait for recovery to complete
  void waitForRecovery();

  // Check if running
  bool isRunning() const;

  // Check if in recovery
  bool isInRecovery() const;

  // Trigger fault (for testing)
  void triggerFault(FaultType type = FaultType::CLIENT_CRASH);

  // Get current sum
  double getSum() const;

  // Get count of processed messages
  int64_t getProcessedCount() const;

  // Get last processed sequence number
  SeqNum getLastSeq() const;

  // Get current state
  ClientState getState() const;

  // Set fault callback
  void setFaultCallback(FaultCallback callback);

  // Enable / disable automatic fault detection (default: enabled)
  void setAutoFaultDetection(bool enabled);

  // Set CPU core for this thread (call before start())
  void setCpuCore(int core_id);

  // Access observability metrics (thread-safe reads)
  const ClientMetrics& getMetrics() const;

 private:
  void run();
  void processMessage(const Msg& msg);
  void onFault(FaultType type);
  void startRecovery();
  void switchToLive(SeqNum expected_seq);

  RingBufferType& buffer_;
  std::string disk_file_;

  std::thread thread_;
  std::atomic<bool> running_;
  std::atomic<bool> stop_requested_;

  // Kahan summation algorithm variables (improve floating point precision)
  std::atomic<double> sum_;
  double kahan_c_;  // Compensation value

  std::atomic<SeqNum> last_seq_;
  std::atomic<int64_t> processed_count_;
  std::atomic<ClientState> state_;
  std::atomic<bool> in_recovery_;

  std::mutex switch_mutex_;
  ConsumerCursor cursor_;

  FaultCallback fault_callback_;

  // Auto fault detection
  std::atomic<bool> auto_fault_detection_{true};

  // Observability
  ClientMetrics metrics_;

  int cpu_core_ = CPU_CORE_UNSET;
};

}  // namespace replay
