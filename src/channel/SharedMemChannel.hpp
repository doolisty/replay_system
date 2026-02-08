#pragma once

#include <memory>

#include "IChannel.hpp"
#include "common/RingBuffer.hpp"

namespace replay {

// Shared memory channel (based on ring buffer)
// Used for real-time data transmission
template <size_t Capacity = DEFAULT_RING_BUFFER_SIZE>
class SharedMemChannel : public IChannel {
 public:
  explicit SharedMemChannel(RingBuffer<Capacity>& buffer,
                            const std::string& name = "SharedMemChannel")
      : buffer_(buffer), name_(name), is_open_(false), cursor_() {}

  ~SharedMemChannel() override { close(); }

  bool open() override {
    is_open_ = true;
    cursor_.reset(0);
    return true;
  }

  void close() override { is_open_ = false; }

  bool isOpen() const override { return is_open_; }

  std::optional<Msg> readNext() override {
    if (!is_open_) {
      return std::nullopt;
    }

    SeqNum seq = cursor_.getReadSeq();
    auto msg = buffer_.read(seq);

    if (msg) {
      cursor_.advance();
    }

    return msg;
  }

  std::optional<Msg> peek() override {
    if (!is_open_) {
      return std::nullopt;
    }

    SeqNum seq = cursor_.getReadSeq();
    return buffer_.read(seq);
  }

  std::string getName() const override { return name_; }

  SeqNum getLatestSeq() const override { return buffer_.getLatestSeq(); }

  bool seek(SeqNum seq) override {
    if (seq < 0) {
      return false;
    }

    // Check if sequence number is within buffer range
    SeqNum latest = buffer_.getLatestSeq();
    SeqNum oldest =
        std::max(SeqNum(0), latest - static_cast<SeqNum>(Capacity) + 1);

    if (seq > latest || seq < oldest) {
      return false;  // Sequence number out of range
    }

    cursor_.setReadSeq(seq);
    return true;
  }

  // Get current read position
  SeqNum getCurrentSeq() const { return cursor_.getReadSeq(); }

  // Set read position
  void setCurrentSeq(SeqNum seq) { cursor_.setReadSeq(seq); }

 private:
  RingBuffer<Capacity>& buffer_;
  std::string name_;
  bool is_open_;
  ConsumerCursor cursor_;
};

}  // namespace replay
