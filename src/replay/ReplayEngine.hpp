#pragma once

#include <functional>
#include <optional>
#include <string>
#include <vector>

#include "channel/FileChannel.hpp"
#include "common/Message.hpp"
#include "common/Types.hpp"

namespace replay {

// Replay engine
// Reads historical messages from disk files, supports catch-up detection and
// switching.
//
// Validates message sequence continuity during replay: each message's seq_num
// must be strictly greater than the previous. Violations are counted and logged.
class ReplayEngine {
 public:
  using CatchUpCallback =
      std::function<void(SeqNum replay_seq, SeqNum live_seq)>;

  explicit ReplayEngine(const std::string& filepath);
  ~ReplayEngine();

  // Disable copy and move
  ReplayEngine(const ReplayEngine&) = delete;
  ReplayEngine& operator=(const ReplayEngine&) = delete;
  ReplayEngine(ReplayEngine&&) = delete;
  ReplayEngine& operator=(ReplayEngine&&) = delete;

  // Open replay file
  bool open();

  // Close replay file
  void close();

  // Check if opened
  bool isOpen() const;

  // Read next message (validates sequence continuity)
  std::optional<Msg> nextMessage();

  // Peek at next message without consuming
  std::optional<Msg> peekMessage();

  // Seek to specified sequence number
  bool seek(SeqNum seq);

  // Reset to beginning
  void reset();

  // Get total message count
  int64_t getMessageCount() const;

  // Get current read position
  SeqNum getCurrentSeq() const;

  // Get sequence number of last message in file
  SeqNum getLastSeq() const;

  // Check if should switch to live source
  bool shouldSwitchToLive(SeqNum live_seq) const;

  // Set catch-up threshold
  void setCatchUpThreshold(int64_t threshold);

  // Set catch-up callback
  void setCatchUpCallback(CatchUpCallback callback);

  // Batch read messages
  std::vector<Msg> readBatch(size_t count);

  // Get file path
  const std::string& getFilePath() const;

  // Whether the file was cleanly closed by the recorder
  bool wasFileCleanlyClose() const;

  // Get first sequence number in the file
  SeqNum getFileFirstSeq() const;

  // Get number of sequence violations detected during replay
  int64_t getSeqViolationCount() const;

 private:
  FileChannel channel_;
  int64_t catchup_threshold_;
  CatchUpCallback catchup_callback_;

  // Validation state
  SeqNum last_read_seq_;       // Last seq_num returned by nextMessage()
  int64_t seq_violation_count_; // Count of sequence order violations
};

}  // namespace replay
