#include "MktDataRecorder.hpp"

#include "common/Logging.hpp"

namespace replay {

MktDataRecorder::MktDataRecorder(RingBufferType& buffer,
                                 const std::string& output_file)
    : buffer_(buffer),
      output_file_(output_file),
      channel_(output_file),
      running_(false),
      stop_requested_(false),
      recorded_count_(0),
      last_seq_(INVALID_SEQ),
      expected_sum_(0.0),
      kahan_c_(0.0),
      batch_size_(DISK_BATCH_SIZE),
      metrics_() {
  batch_buffer_.reserve(batch_size_);
}

MktDataRecorder::~MktDataRecorder() { stop(); }

void MktDataRecorder::start() {
  if (running_) {
    LOG_WARNING(replay::logger(),
                "MktDataRecorder already running, ignoring start {}", "");
    return;
  }

  if (!channel_.open()) {
    LOG_ERROR(replay::logger(), "Failed to open output file: {}", output_file_);
    return;  // Cannot open output file
  }

  stop_requested_ = false;
  recorded_count_ = 0;
  last_seq_ = INVALID_SEQ;
  expected_sum_ = 0.0;
  kahan_c_ = 0.0;
  running_ = true;

  LOG_INFO(replay::logger(), "MktDataRecorder start: output={}, batch_size={}",
           output_file_, batch_size_);

  thread_ = std::thread(&MktDataRecorder::run, this);
}

void MktDataRecorder::stop() {
  stop_requested_ = true;

  if (thread_.joinable()) {
    thread_.join();
  }

  // Write remaining data
  writeBatch();
  channel_.close();  // Sets FILE_FLAG_COMPLETE

  running_ = false;
  LOG_INFO(replay::logger(),
           "MktDataRecorder stopped: recorded={}, gaps={}, overwrites={}",
           getRecordedCount(),
           metrics_.seq_gap_count.load(std::memory_order_relaxed),
           metrics_.overwrite_count.load(std::memory_order_relaxed));
}

void MktDataRecorder::waitForComplete() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool MktDataRecorder::isRunning() const { return running_; }

int64_t MktDataRecorder::getRecordedCount() const {
  return recorded_count_.load(std::memory_order_acquire);
}

SeqNum MktDataRecorder::getLastSeq() const {
  return last_seq_.load(std::memory_order_acquire);
}

double MktDataRecorder::getExpectedSum() const {
  return expected_sum_.load(std::memory_order_acquire);
}

void MktDataRecorder::flush() {
  writeBatch();
  channel_.flush();
}

void MktDataRecorder::setBatchSize(size_t size) {
  batch_size_ = size;
  batch_buffer_.reserve(size);
}

const RecorderMetrics& MktDataRecorder::getMetrics() const { return metrics_; }

void MktDataRecorder::setCpuCore(int core_id) { cpu_core_ = core_id; }

// ---------------------------------------------------------------------------
// Main recorder loop.
//
// Uses readEx() to detect overwrites. The recorder is the most critical
// consumer — if it gets lapped, we lose data permanently. In production,
// the ring buffer should be sized and the recorder prioritized so that this
// never happens. When it does, we log an error, count the gap, and skip
// ahead to the next available message.
// ---------------------------------------------------------------------------
void MktDataRecorder::run() {
  setCpuAffinity(cpu_core_, "MktDataRecorder");

  cursor_.reset(0);

  while (!stop_requested_) {
    SeqNum seq = cursor_.getReadSeq();
    auto result = buffer_.readEx(seq);

    switch (result.status) {
      case ReadStatus::OK: {
        // INV-R1: Verify monotonic sequence
        SeqNum prev = last_seq_.load(std::memory_order_relaxed);
        if (prev != INVALID_SEQ && result.msg.seq_num <= prev) {
          LOG_WARNING(replay::logger(),
                      "Recorder: duplicate/out-of-order seq={}, prev={}",
                      result.msg.seq_num, prev);
          cursor_.advance();
          break;
        }
        if (prev != INVALID_SEQ && result.msg.seq_num != prev + 1) {
          int64_t gap = result.msg.seq_num - prev - 1;
          metrics_.seq_gap_count.fetch_add(gap, std::memory_order_relaxed);
          LOG_WARNING(replay::logger(),
                      "Recorder: seq gap detected, expected={}, got={}, gap={}",
                      prev + 1, result.msg.seq_num, gap);
        }

        batch_buffer_.push_back(result.msg);

        // Kahan summation
        double y = result.msg.payload - kahan_c_;
        double current_sum = expected_sum_.load(std::memory_order_relaxed);
        double t = current_sum + y;
        kahan_c_ = (t - current_sum) - y;
        expected_sum_.store(t, std::memory_order_release);

        last_seq_.store(result.msg.seq_num, std::memory_order_release);
        recorded_count_.fetch_add(1, std::memory_order_release);
        cursor_.advance();

        // Batch write
        if (batch_buffer_.size() >= batch_size_) {
          writeBatch();
        }
        break;
      }

      case ReadStatus::OVERWRITTEN: {
        // Critical: recorder was lapped. Log error, skip ahead.
        metrics_.overwrite_count.fetch_add(1, std::memory_order_relaxed);
        LOG_ERROR(replay::logger(),
                  "CRITICAL: Recorder lapped by producer at seq={}. "
                  "Data loss is permanent. Consider increasing buffer size.",
                  seq);

        // Skip to next writable position
        SeqNum latest = buffer_.getLatestSeq();
        if (latest >= 0) {
          // Jump close to head but leave some margin
          SeqNum new_pos = std::max(
              seq + 1,
              latest - static_cast<SeqNum>(buffer_.capacity()) / 2);
          cursor_.setReadSeq(new_pos);
        } else {
          cursor_.advance();
        }

        // Flush what we have before the gap
        if (!batch_buffer_.empty()) {
          writeBatch();
        }
        break;
      }

      case ReadStatus::NOT_READY:
        // No new messages
        if (!batch_buffer_.empty()) {
          // Has data to write, write to disk
          writeBatch();
        }
        std::this_thread::yield();
        break;
    }
  }

  running_ = false;
  LOG_INFO(replay::logger(), "MktDataRecorder completed: recorded={}",
           getRecordedCount());
}

void MktDataRecorder::writeBatch() {
  for (const auto& msg : batch_buffer_) {
    channel_.write(msg);
  }
  batch_buffer_.clear();
  channel_.flush();
}

}  // namespace replay
