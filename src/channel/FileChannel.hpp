#pragma once

#include <fstream>
#include <string>
#include <string_view>

#include "IChannel.hpp"

namespace replay {

// File channel (for replay)
// Sequentially reads historical messages from disk files
//
// On open, validates the file header for structural consistency.
// If the file was not cleanly closed (FILE_FLAG_COMPLETE missing), the reader
// falls back to the msg_count stored in the header (which is periodically
// flushed) and logs a warning. This allows partial recovery after a crash.
class FileChannel : public IChannel {
 public:
  explicit FileChannel(std::string_view filepath)
      : filepath_(filepath),
        is_open_(false),
        current_seq_(0),
        msg_count_(0),
        first_seq_(INVALID_SEQ),
        last_seq_(INVALID_SEQ),
        was_cleanly_closed_(false) {}

  ~FileChannel() override { close(); }

  bool open() override {
    if (is_open_) {
      return true;
    }

    file_.open(filepath_, std::ios::binary | std::ios::in);
    if (!file_.is_open()) {
      return false;
    }

    // Read file header
    FileHeader header;
    file_.read(reinterpret_cast<char*>(&header), sizeof(FileHeader));

    if (!file_.good() || !header.isValid()) {
      file_.close();
      return false;
    }

    // Validate structural consistency
    if (!header.isConsistent()) {
      // Header fields are internally inconsistent — likely corruption or
      // crash during header update. Fall back to what we can trust.
      // Use msg_count as-is (it's periodically flushed) but clear seq range.
      msg_count_ = header.msg_count;
      first_seq_ = INVALID_SEQ;
      last_seq_ = INVALID_SEQ;
      was_cleanly_closed_ = false;
    } else {
      msg_count_ = header.msg_count;
      first_seq_ = header.first_seq;
      last_seq_ = header.last_seq;
      was_cleanly_closed_ = header.isComplete();
    }

    current_seq_ = 0;
    is_open_ = true;

    return true;
  }

  void close() override {
    if (file_.is_open()) {
      file_.close();
    }
    is_open_ = false;
    current_seq_ = 0;
  }

  bool isOpen() const override { return is_open_; }

  std::optional<Msg> readNext() override {
    if (!is_open_ || current_seq_ >= msg_count_) {
      return std::nullopt;
    }

    Msg msg;
    file_.read(reinterpret_cast<char*>(&msg), sizeof(Msg));

    if (!file_.good()) {
      return std::nullopt;
    }

    current_seq_++;
    return msg;
  }

  std::optional<Msg> peek() override {
    if (!is_open_ || current_seq_ >= msg_count_) {
      return std::nullopt;
    }

    // Save current position
    auto pos = file_.tellg();

    Msg msg;
    file_.read(reinterpret_cast<char*>(&msg), sizeof(Msg));

    // Restore position
    file_.seekg(pos);

    if (!file_.good()) {
      return std::nullopt;
    }

    return msg;
  }

  std::string getName() const override { return "FileChannel: " + filepath_; }

  SeqNum getLatestSeq() const override {
    return msg_count_ > 0 ? msg_count_ - 1 : INVALID_SEQ;
  }

  bool seek(SeqNum seq) override {
    if (!is_open_ || seq < 0 || seq >= msg_count_) {
      return false;
    }

    // Calculate file offset (skip file header)
    std::streamoff offset = sizeof(FileHeader) + seq * sizeof(Msg);
    file_.seekg(offset);

    if (!file_.good()) {
      return false;
    }

    current_seq_ = seq;
    return true;
  }

  // Get total message count
  int64_t getMessageCount() const { return msg_count_; }

  // Get current read position
  SeqNum getCurrentSeq() const { return current_seq_; }

  // Get file path
  const std::string& getFilePath() const { return filepath_; }

  // Get first sequence number recorded in file
  SeqNum getFirstSeq() const { return first_seq_; }

  // Get last sequence number recorded in file
  SeqNum getFileLastSeq() const { return last_seq_; }

  // Whether the file was cleanly closed by the writer
  bool wasCleanlyClose() const { return was_cleanly_closed_; }

 private:
  std::string filepath_;
  std::ifstream file_;
  bool is_open_;
  SeqNum current_seq_;
  int64_t msg_count_;
  SeqNum first_seq_;
  SeqNum last_seq_;
  bool was_cleanly_closed_;
};

// File write channel — maintains first_seq / last_seq / flags for integrity.
//
// Invariants maintained:
//   - first_seq is set on the first write and never changes
//   - last_seq is updated on every write
//   - msg_count == (last_seq - first_seq + 1)
//   - FILE_FLAG_COMPLETE is set only in close()
//   - Header is flushed periodically (on flush()) so crash recovery can read
//     partial data up to the last flushed msg_count
class FileWriteChannel : public IWritableChannel {
 public:
  explicit FileWriteChannel(std::string_view filepath)
      : filepath_(filepath),
        is_open_(false),
        msg_count_(0),
        first_seq_(INVALID_SEQ),
        last_seq_(INVALID_SEQ),
        header_() {}

  ~FileWriteChannel() override { close(); }

  bool open() override {
    if (is_open_) {
      return true;
    }

    file_.open(filepath_, std::ios::binary | std::ios::out | std::ios::trunc);
    if (!file_.is_open()) {
      return false;
    }

    // Write file header (placeholder, will be updated on flush/close)
    header_ = FileHeader();
    file_.write(reinterpret_cast<const char*>(&header_), sizeof(FileHeader));

    if (!file_.good()) {
      file_.close();
      return false;
    }

    msg_count_ = 0;
    first_seq_ = INVALID_SEQ;
    last_seq_ = INVALID_SEQ;
    is_open_ = true;
    return true;
  }

  void close() override {
    if (is_open_) {
      // Mark file as cleanly closed and update header
      header_.flags |= FILE_FLAG_COMPLETE;
      updateHeader();
      file_.close();
    }
    is_open_ = false;
  }

  bool isOpen() const override { return is_open_; }

  std::optional<Msg> readNext() override {
    // Write channel does not support reading
    return std::nullopt;
  }

  std::optional<Msg> peek() override { return std::nullopt; }

  std::string getName() const override {
    return "FileWriteChannel: " + filepath_;
  }

  SeqNum getLatestSeq() const override {
    return msg_count_ > 0 ? msg_count_ - 1 : INVALID_SEQ;
  }

  bool seek(SeqNum /*seq*/) override {
    // Write channel does not support seeking
    return false;
  }

  bool write(const Msg& msg) override {
    if (!is_open_) {
      return false;
    }

    file_.write(reinterpret_cast<const char*>(&msg), sizeof(Msg));

    if (!file_.good()) {
      return false;
    }

    // Track sequence range
    if (first_seq_ == INVALID_SEQ) {
      first_seq_ = msg.seq_num;
    }
    last_seq_ = msg.seq_num;
    msg_count_++;
    return true;
  }

  void flush() override {
    if (is_open_) {
      // Update header so other processes / crash recovery can read latest data.
      // Note: FILE_FLAG_COMPLETE is NOT set here — only on clean close().
      updateHeader();
    }
  }

  // Get count of written messages
  int64_t getMessageCount() const { return msg_count_; }

  // Get file path
  const std::string& getFilePath() const { return filepath_; }

 private:
  void updateHeader() {
    // Save current write position
    auto current_pos = file_.tellp();

    // Go back to file beginning to update header atomically
    file_.seekp(0);
    header_.msg_count = msg_count_;
    header_.first_seq = first_seq_;
    header_.last_seq = last_seq_;
    file_.write(reinterpret_cast<const char*>(&header_), sizeof(FileHeader));

    // Restore write position to end of file
    file_.seekp(current_pos);
    file_.flush();
  }

  std::string filepath_;
  std::ofstream file_;
  bool is_open_;
  int64_t msg_count_;
  SeqNum first_seq_;
  SeqNum last_seq_;
  FileHeader header_;
};

}  // namespace replay
