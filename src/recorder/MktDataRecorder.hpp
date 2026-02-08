#pragma once

#include <atomic>
#include <string>
#include <thread>
#include <vector>

#include "channel/FileChannel.hpp"
#include "common/CpuAffinity.hpp"
#include "common/Message.hpp"
#include "common/RingBuffer.hpp"

namespace replay {

// Observability metrics for the recorder
struct RecorderMetrics {
  std::atomic<int64_t> seq_gap_count{0};    // Sequence gaps detected
  std::atomic<int64_t> overwrite_count{0};  // Ring buffer overwrites detected
};

// Market data recorder
// Independent thread consumes messages and persists to disk files
//
// Correctness invariant:
//   INV-R1: Messages are written to disk in strictly increasing seq_num order
//           with no gaps. If a gap is detected (ring buffer overwrite), it is
//           logged and counted but recording continues — the gap will be
//           visible in the file's seq_num stream and the header's first_seq /
//           last_seq will reflect the actual range.
class MktDataRecorder {
 public:
  using RingBufferType = RingBuffer<DEFAULT_RING_BUFFER_SIZE>;

  MktDataRecorder(RingBufferType& buffer, const std::string& output_file);
  ~MktDataRecorder();

  // Disable copy and move
  MktDataRecorder(const MktDataRecorder&) = delete;
  MktDataRecorder& operator=(const MktDataRecorder&) = delete;
  MktDataRecorder(MktDataRecorder&&) = delete;
  MktDataRecorder& operator=(MktDataRecorder&&) = delete;

  // Start recorder
  void start();

  // Stop recorder
  void stop();

  // Wait for recording to complete
  void waitForComplete();

  // Check if running
  bool isRunning() const;

  // Get count of recorded messages
  int64_t getRecordedCount() const;

  // Get last recorded sequence number
  SeqNum getLastSeq() const;

  // Get expected sum (for verification)
  double getExpectedSum() const;

  // Flush buffer to disk
  void flush();

  // Set batch write size
  void setBatchSize(size_t size);

  // Set CPU core for this thread (call before start())
  void setCpuCore(int core_id);

  // Access observability metrics (thread-safe reads)
  const RecorderMetrics& getMetrics() const;

 private:
  void run();
  void writeBatch();

  RingBufferType& buffer_;
  std::string output_file_;
  FileWriteChannel channel_;

  std::thread thread_;
  std::atomic<bool> running_;
  std::atomic<bool> stop_requested_;

  std::atomic<int64_t> recorded_count_;
  std::atomic<SeqNum> last_seq_;
  std::atomic<double> expected_sum_;
  double kahan_c_;

  std::vector<Msg> batch_buffer_;
  size_t batch_size_;

  ConsumerCursor cursor_;

  // Observability
  RecorderMetrics metrics_;

  int cpu_core_ = CPU_CORE_UNSET;
};

}  // namespace replay
