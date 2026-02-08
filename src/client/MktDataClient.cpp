#include "MktDataClient.hpp"

#include <cassert>
#include <chrono>

#include "common/Logging.hpp"
#include "replay/ReplayEngine.hpp"

namespace replay {

MktDataClient::MktDataClient(RingBufferType& buffer,
                             const std::string& disk_file)
    : buffer_(buffer),
      disk_file_(disk_file),
      running_(false),
      stop_requested_(false),
      sum_(0.0),
      kahan_c_(0.0),
      last_seq_(INVALID_SEQ),
      processed_count_(0),
      state_(ClientState::NORMAL),
      in_recovery_(false),
      fault_callback_(nullptr),
      auto_fault_detection_(true),
      metrics_() {}

MktDataClient::~MktDataClient() { stop(); }

void MktDataClient::start() {
  if (running_) {
    LOG_WARNING(replay::logger(),
                "MktDataClient already running, ignoring start {}", "");
    return;
  }

  stop_requested_ = false;
  running_ = true;
  state_ = ClientState::NORMAL;

  LOG_INFO(replay::logger(), "MktDataClient start {}", "");
  thread_ = std::thread(&MktDataClient::run, this);
}

void MktDataClient::stop() {
  stop_requested_ = true;

  if (thread_.joinable()) {
    thread_.join();
  }

  running_ = false;
  LOG_INFO(replay::logger(),
           "MktDataClient stopped: processed={}, gaps={}, overwrites={}, "
           "recoveries={}",
           getProcessedCount(),
           metrics_.seq_gap_count.load(std::memory_order_relaxed),
           metrics_.overwrite_count.load(std::memory_order_relaxed),
           metrics_.recovery_count.load(std::memory_order_relaxed));
}

void MktDataClient::waitForRecovery() {
  while (in_recovery_) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
}

bool MktDataClient::isRunning() const { return running_; }

bool MktDataClient::isInRecovery() const { return in_recovery_; }

void MktDataClient::triggerFault(FaultType type) { onFault(type); }

double MktDataClient::getSum() const {
  return sum_.load(std::memory_order_acquire);
}

int64_t MktDataClient::getProcessedCount() const {
  return processed_count_.load(std::memory_order_acquire);
}

SeqNum MktDataClient::getLastSeq() const {
  return last_seq_.load(std::memory_order_acquire);
}

ClientState MktDataClient::getState() const {
  return state_.load(std::memory_order_acquire);
}

void MktDataClient::setFaultCallback(FaultCallback callback) {
  fault_callback_ = std::move(callback);
}

void MktDataClient::setAutoFaultDetection(bool enabled) {
  auto_fault_detection_.store(enabled, std::memory_order_relaxed);
}

const ClientMetrics& MktDataClient::getMetrics() const { return metrics_; }

void MktDataClient::setCpuCore(int core_id) { cpu_core_ = core_id; }

// ---------------------------------------------------------------------------
// Main consumer loop
//
// Uses readEx() to distinguish "not ready" from "overwritten". When an
// overwrite is detected, the consumer knows it has been lapped by the producer
// and triggers automatic recovery (if enabled), since the missing messages
// can only be recovered from disk.
// ---------------------------------------------------------------------------
void MktDataClient::run() {
  setCpuAffinity(cpu_core_, "MktDataClient");

  cursor_.reset(0);

  while (!stop_requested_) {
    if (in_recovery_) {
      // Recovery mode: wait for recovery to complete
      std::this_thread::sleep_for(std::chrono::milliseconds(1));
      continue;
    }

    SeqNum seq = cursor_.getReadSeq();
    auto result = buffer_.readEx(seq);

    switch (result.status) {
      case ReadStatus::OK:
        processMessage(result.msg);
        cursor_.advance();
        break;

      case ReadStatus::OVERWRITTEN:
        // The producer has lapped us — we lost one or more messages.
        metrics_.overwrite_count.fetch_add(1, std::memory_order_relaxed);
        metrics_.seq_gap_count.fetch_add(1, std::memory_order_relaxed);
        LOG_WARNING(replay::logger(),
                    "Ring buffer overwrite detected at seq={}, triggering "
                    "recovery", seq);

        if (auto_fault_detection_.load(std::memory_order_relaxed) &&
            !in_recovery_) {
          metrics_.auto_fault_count.fetch_add(1, std::memory_order_relaxed);
          onFault(FaultType::CLIENT_CRASH);
        } else {
          // Skip to latest available position if auto-recovery is off
          SeqNum latest = buffer_.getLatestSeq();
          if (latest >= 0) {
            cursor_.setReadSeq(latest + 1);
          }
        }
        break;

      case ReadStatus::NOT_READY:
        // No new messages, wait briefly
        std::this_thread::yield();
        break;
    }
  }

  running_ = false;
}

// ---------------------------------------------------------------------------
// Process a single message with Kahan summation.
//
// INV-C1 check: verify that seq_num is strictly greater than the last
// processed seq. If not, we log a warning and skip the duplicate.
// ---------------------------------------------------------------------------
void MktDataClient::processMessage(const Msg& msg) {
  SeqNum prev_seq = last_seq_.load(std::memory_order_relaxed);

  // INV-C1: Monotonic sequence check
  if (prev_seq != INVALID_SEQ && msg.seq_num <= prev_seq) {
    // Duplicate or out-of-order message — skip to maintain invariant.
    LOG_WARNING(replay::logger(),
                "Sequence monotonicity violation: prev={}, got={}", prev_seq,
                msg.seq_num);
    metrics_.seq_gap_count.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  // Detect gaps (missing sequences) — informational, not fatal
  if (prev_seq != INVALID_SEQ && msg.seq_num != prev_seq + 1) {
    int64_t gap = msg.seq_num - prev_seq - 1;
    metrics_.seq_gap_count.fetch_add(gap, std::memory_order_relaxed);
    LOG_WARNING(replay::logger(),
                "Sequence gap detected: expected={}, got={}, gap={}",
                prev_seq + 1, msg.seq_num, gap);
  }

  // Kahan summation algorithm
  double y = msg.payload - kahan_c_;
  double current_sum = sum_.load(std::memory_order_relaxed);
  double t = current_sum + y;
  kahan_c_ = (t - current_sum) - y;
  sum_.store(t, std::memory_order_release);

  last_seq_.store(msg.seq_num, std::memory_order_release);
  processed_count_.fetch_add(1, std::memory_order_release);
}

void MktDataClient::onFault(FaultType type) {
  switch (type) {
    case FaultType::CLIENT_CRASH:
      LOG_WARNING(replay::logger(),
                  "Client fault: CLIENT_CRASH, starting recovery {}", "");
      // Simulate crash: reset accumulated value
      state_.store(ClientState::FAULTED, std::memory_order_release);
      sum_.store(0.0, std::memory_order_release);
      kahan_c_ = 0.0;
      last_seq_.store(INVALID_SEQ, std::memory_order_release);
      processed_count_.store(0, std::memory_order_release);

      if (fault_callback_) {
        fault_callback_();
      }

      startRecovery();
      break;

    case FaultType::MESSAGE_LOSS:
      LOG_WARNING(replay::logger(),
                  "Client fault: MESSAGE_LOSS, skipping messages {}", "");
      // Simulate message loss: skip some messages
      cursor_.setReadSeq(cursor_.getReadSeq() + 10);
      break;

    case FaultType::TEMPORARY_HANG:
      LOG_WARNING(replay::logger(), "Client fault: TEMPORARY_HANG {}", "");
      // Simulate temporary hang
      std::this_thread::sleep_for(std::chrono::seconds(1));
      break;
  }
}

// ---------------------------------------------------------------------------
// Recovery procedure: replay from disk, then switch to live ring buffer.
//
// The key correctness argument for the replay→live handoff:
//
//   1. We replay all messages from disk sequentially (seq 0, 1, 2, ... N).
//      Let last_replay_seq = N.
//   2. We set cursor to last_replay_seq + 1 and begin reading from the ring
//      buffer.
//   3. The ring buffer is sized so that during the time we replay from disk,
//      it retains messages from at least (live_seq - Capacity + 1) onward.
//   4. The CATCHUP_THRESHOLD ensures we switch while last_replay_seq + 1 is
//      still within the ring buffer window, i.e.,
//          last_replay_seq + 1 >= live_seq - Capacity + 1
//      which is guaranteed when CATCHUP_THRESHOLD << Capacity.
//   5. Therefore no message is missed at the boundary (INV-C2).
//
// If the ring buffer has already overwritten past last_replay_seq + 1 by the
// time we switch, we fall through and the main loop's OVERWRITTEN detection
// will re-trigger recovery. This is safe but indicates the buffer is too small
// for the workload — an operational alert should fire.
// ---------------------------------------------------------------------------
void MktDataClient::startRecovery() {
  in_recovery_.store(true, std::memory_order_release);
  state_.store(ClientState::REPLAYING, std::memory_order_release);
  metrics_.recovery_count.fetch_add(1, std::memory_order_relaxed);

  LOG_INFO(replay::logger(), "Client recovery started, replaying from disk: {}",
           disk_file_);
  ReplayEngine replay(disk_file_);

  if (!replay.open()) {
    LOG_ERROR(replay::logger(), "Failed to open replay file: {}", disk_file_);
    // Cannot open replay file, start directly from current position
    in_recovery_.store(false, std::memory_order_release);
    state_.store(ClientState::NORMAL, std::memory_order_release);
    return;
  }

  SeqNum last_recovered_seq = INVALID_SEQ;
  bool switched_to_live = false;

  // Replay from the beginning
  while (in_recovery_ && !stop_requested_) {
    auto msg = replay.nextMessage();

    if (!msg) {
      // Replay complete — all recorded messages consumed
      break;
    }

    processMessage(*msg);
    last_recovered_seq = msg->seq_num;

    // Check if can switch to live source
    SeqNum live_seq = buffer_.getLatestSeq();

    if (live_seq >= 0 && msg->seq_num >= live_seq - CATCHUP_THRESHOLD) {
      state_.store(ClientState::CATCHING_UP, std::memory_order_release);

      SeqNum boundary_seq = msg->seq_num + 1;

      // INV-C2 verification: the next live message we will read must be
      // exactly boundary_seq. switchToLive positions the cursor there.
      switchToLive(boundary_seq);
      switched_to_live = true;

      LOG_INFO(replay::logger(),
               "Replay-to-live boundary: last_replay_seq={}, "
               "first_live_seq={}, live_head={}",
               msg->seq_num, boundary_seq, live_seq);
      break;
    }
  }

  replay.close();

  // If not switched via switchToLive, need to manually set cursor position
  // Ensure continue reading ring buffer from the last position read from disk
  if (!switched_to_live && last_recovered_seq != INVALID_SEQ) {
    cursor_.setReadSeq(last_recovered_seq + 1);
    LOG_INFO(replay::logger(),
             "Replay exhausted disk, resuming from seq={} (no live switch)",
             last_recovered_seq + 1);
  }

  in_recovery_.store(false, std::memory_order_release);
  state_.store(ClientState::NORMAL, std::memory_order_release);
  LOG_INFO(replay::logger(), "Client recovery finished: last_seq={}",
           last_recovered_seq);
}

// ---------------------------------------------------------------------------
// Switch from replay to live ring buffer.
//
// expected_seq is the first sequence number we need from the live stream.
// We set the cursor directly to expected_seq. The ring buffer's readEx()
// will return OVERWRITTEN if this position has already been lapped, which
// will trigger another recovery cycle — a safe fallback.
// ---------------------------------------------------------------------------
void MktDataClient::switchToLive(SeqNum expected_seq) {
  std::lock_guard<std::mutex> lock(switch_mutex_);

  // Verify the target position is still within the ring buffer window
  SeqNum latest = buffer_.getLatestSeq();
  SeqNum oldest_available =
      std::max(SeqNum(0),
               latest - static_cast<SeqNum>(RingBufferType::capacity()) + 1);

  if (expected_seq < oldest_available) {
    LOG_WARNING(replay::logger(),
                "switchToLive: expected_seq={} already overwritten "
                "(oldest_available={}), will re-trigger recovery",
                expected_seq, oldest_available);
  }

  // Set read position directly — the main loop handles OVERWRITTEN gracefully
  cursor_.setReadSeq(expected_seq);
  LOG_INFO(replay::logger(),
           "Client switched to live: expected_seq={}, buffer_range=[{}, {}]",
           expected_seq, oldest_available, latest);
}

}  // namespace replay
