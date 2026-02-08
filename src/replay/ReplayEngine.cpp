#include "ReplayEngine.hpp"

#include "common/Logging.hpp"

namespace replay {

ReplayEngine::ReplayEngine(const std::string& filepath)
    : channel_(filepath),
      catchup_threshold_(CATCHUP_THRESHOLD),
      catchup_callback_(nullptr),
      last_read_seq_(INVALID_SEQ),
      seq_violation_count_(0) {}

ReplayEngine::~ReplayEngine() { close(); }

bool ReplayEngine::open() {
  bool ok = channel_.open();
  if (ok) {
    last_read_seq_ = INVALID_SEQ;
    seq_violation_count_ = 0;

    if (!channel_.wasCleanlyClose()) {
      LOG_WARNING(replay::logger(),
                  "Replay file was NOT cleanly closed (possible crash). "
                  "Data may be truncated: {}",
                  channel_.getFilePath());
    }
  }
  return ok;
}

void ReplayEngine::close() { channel_.close(); }

bool ReplayEngine::isOpen() const { return channel_.isOpen(); }

// Read next message with sequence continuity validation.
// If the file contains out-of-order or duplicate sequences, we log a warning
// and count violations but still return the message — the consumer decides
// whether to skip or process.
std::optional<Msg> ReplayEngine::nextMessage() {
  auto msg = channel_.readNext();
  if (msg) {
    if (last_read_seq_ != INVALID_SEQ && msg->seq_num <= last_read_seq_) {
      seq_violation_count_++;
      LOG_WARNING(replay::logger(),
                  "Replay sequence violation: prev={}, got={} in file {}",
                  last_read_seq_, msg->seq_num, channel_.getFilePath());
    }
    last_read_seq_ = msg->seq_num;
  }
  return msg;
}

std::optional<Msg> ReplayEngine::peekMessage() { return channel_.peek(); }

bool ReplayEngine::seek(SeqNum seq) {
  bool ok = channel_.seek(seq);
  if (ok) {
    // Reset validation state after seek — we can't verify continuity
    // across a seek boundary
    last_read_seq_ = INVALID_SEQ;
  }
  return ok;
}

void ReplayEngine::reset() {
  channel_.seek(0);
  last_read_seq_ = INVALID_SEQ;
}

int64_t ReplayEngine::getMessageCount() const {
  return channel_.getMessageCount();
}

SeqNum ReplayEngine::getCurrentSeq() const { return channel_.getCurrentSeq(); }

SeqNum ReplayEngine::getLastSeq() const { return channel_.getLatestSeq(); }

bool ReplayEngine::shouldSwitchToLive(SeqNum live_seq) const {
  SeqNum current = channel_.getCurrentSeq();
  if (current < 0) {
    return false;
  }

  bool should_switch = (live_seq - current) <= catchup_threshold_;

  if (should_switch && catchup_callback_) {
    catchup_callback_(current, live_seq);
  }

  return should_switch;
}

void ReplayEngine::setCatchUpThreshold(int64_t threshold) {
  catchup_threshold_ = threshold;
}

void ReplayEngine::setCatchUpCallback(CatchUpCallback callback) {
  catchup_callback_ = std::move(callback);
}

std::vector<Msg> ReplayEngine::readBatch(size_t count) {
  std::vector<Msg> batch;
  batch.reserve(count);

  for (size_t i = 0; i < count; ++i) {
    auto msg = nextMessage();
    if (!msg) {
      break;
    }
    batch.push_back(*msg);
  }

  return batch;
}

const std::string& ReplayEngine::getFilePath() const {
  return channel_.getFilePath();
}

bool ReplayEngine::wasFileCleanlyClose() const {
  return channel_.wasCleanlyClose();
}

SeqNum ReplayEngine::getFileFirstSeq() const {
  return channel_.getFirstSeq();
}

int64_t ReplayEngine::getSeqViolationCount() const {
  return seq_violation_count_;
}

}  // namespace replay
