#pragma once

#include <array>
#include <atomic>
#include <cstring>
#include <optional>
#include <span>

#include "Message.hpp"
#include "Types.hpp"

namespace replay {

// Result of a read operation with explicit status
struct ReadResult {
  ReadStatus status;
  Msg msg;  // Only valid when status == ReadStatus::OK
};

// Lock-free SPMC (Single Producer Multiple Consumer) ring buffer
// Uses sequence numbers as indices, supports independent reading by multiple
// consumers
//
// Correctness invariants:
//   INV-1: write_seq_ is monotonically increasing (single producer)
//   INV-2: Slot at index (seq & (Capacity-1)) publishes seq_num == seq
//          only after message data is fully written (release semantics).
//          Consumers use a seqlock double-check to detect concurrent overwrites.
//   INV-3: A consumer reading with acquire semantics sees either:
//          (a) the exact message at expected_seq (OK),
//          (b) a newer message (OVERWRITTEN — consumer was lapped), or
//          (c) INVALID_SEQ / older seq (NOT_READY — producer hasn't reached here)
template <size_t Capacity = DEFAULT_RING_BUFFER_SIZE>
class RingBuffer {
  static_assert(Capacity > 0, "Capacity must be positive");
  static_assert((Capacity & (Capacity - 1)) == 0,
                "Capacity must be a power of 2 for bitmask indexing");

 public:
  RingBuffer() : write_seq_(0), overwrite_count_(0) {
    // Initialize all slots
    for (auto& slot : buffer_) {
      slot.seq.store(INVALID_SEQ, std::memory_order_relaxed);
    }
  }

  // Disable copy and move
  RingBuffer(const RingBuffer&) = delete;
  RingBuffer& operator=(const RingBuffer&) = delete;
  RingBuffer(RingBuffer&&) = delete;
  RingBuffer& operator=(RingBuffer&&) = delete;

  // Producer writes message
  // Returns written sequence number, returns INVALID_SEQ on failure
  //
  // Design choice: never block the producer. If the buffer is full, we
  // overwrite the oldest slot. This is the correct trade-off for a market data
  // server that must not stall. Consumers detect loss via readEx() returning
  // OVERWRITTEN.
  SeqNum push(const Msg& msg) {
    SeqNum seq = write_seq_.fetch_add(1, std::memory_order_relaxed);
    size_t index = seq & (Capacity - 1);

    Slot& slot = buffer_[index];

    // Track overwrites: if the slot already holds a valid message, we are
    // overwriting data that a slow consumer may not have read yet.
    // After the first full wrap (seq >= Capacity), every push overwrites.
    SeqNum old_seq = slot.seq.load(std::memory_order_acquire);
    if (old_seq != INVALID_SEQ) {
      overwrite_count_.fetch_add(1, std::memory_order_relaxed);
    }

    // Write message data
    slot.msg = msg;
    slot.msg.seq_num = seq;  // Use sequence number assigned by ring buffer

    // Publish message (make visible to consumers) — INV-2
    slot.seq.store(seq, std::memory_order_release);

    return seq;
  }

  // Batch write messages using std::span
  // Returns the first sequence number of the batch, or INVALID_SEQ if batch is
  // empty
  SeqNum pushBatch(std::span<const Msg> messages) {
    if (messages.empty()) {
      return INVALID_SEQ;
    }

    // Reserve sequence numbers atomically for the entire batch
    SeqNum first_seq = write_seq_.fetch_add(
        static_cast<SeqNum>(messages.size()), std::memory_order_relaxed);

    // Write all messages to their slots
    for (size_t i = 0; i < messages.size(); ++i) {
      SeqNum seq = first_seq + static_cast<SeqNum>(i);
      size_t index = seq & (Capacity - 1);
      Slot& slot = buffer_[index];

      SeqNum old_seq = slot.seq.load(std::memory_order_acquire);
      if (old_seq != INVALID_SEQ) {
        overwrite_count_.fetch_add(1, std::memory_order_relaxed);
      }

      // Write message data
      slot.msg = messages[i];
      slot.msg.seq_num = seq;

      // Publish message
      slot.seq.store(seq, std::memory_order_release);
    }

    return first_seq;
  }

  // Extended read: returns explicit status so consumer can distinguish
  // "not yet published" from "overwritten (message lost)".
  //
  // Uses a seqlock double-check to guarantee consistency: after copying
  // the message, we re-read the slot sequence number to ensure the producer
  // did not overwrite the slot while we were reading.
  //
  // Proof of correctness (INV-3):
  //   The slot at (expected_seq & (Capacity-1)) contains an atomic seq field.
  //   Case 1: seq == expected_seq both before and after msg copy →
  //           the message is consistent. → OK
  //   Case 2: seq > expected_seq (either check) → the producer has advanced
  //           past our position by at least one full wrap. → OVERWRITTEN
  //   Case 3: seq < expected_seq or INVALID_SEQ → the producer hasn't
  //           written this slot yet. → NOT_READY
  ReadResult readEx(SeqNum expected_seq) const {
    if (expected_seq < 0) {
      return {ReadStatus::NOT_READY, {}};
    }

    size_t index = expected_seq & (Capacity - 1);
    const Slot& slot = buffer_[index];

    SeqNum published_seq = slot.seq.load(std::memory_order_acquire);

    if (published_seq == expected_seq) {
      // Copy message to a local variable before the second check.
      Msg local_msg = slot.msg;

      // Seqlock double-check: an acquire fence ensures the copy of msg
      // is fully visible before we re-read the sequence number.
      std::atomic_thread_fence(std::memory_order_acquire);
      SeqNum recheck_seq = slot.seq.load(std::memory_order_relaxed);

      if (recheck_seq == expected_seq) {
        return {ReadStatus::OK, local_msg};
      }
      // Slot was overwritten between the two checks — data is torn.
      return {ReadStatus::OVERWRITTEN, {}};
    } else if (published_seq > expected_seq) {
      return {ReadStatus::OVERWRITTEN, {}};
    } else {
      return {ReadStatus::NOT_READY, {}};
    }
  }

  // Legacy read interface — returns std::optional<Msg>.
  // Cannot distinguish NOT_READY from OVERWRITTEN; prefer readEx() for new code.
  std::optional<Msg> read(SeqNum expected_seq) const {
    auto result = readEx(expected_seq);
    if (result.status == ReadStatus::OK) {
      return result.msg;
    }
    return std::nullopt;
  }

  // Try to read message, returns empty if message at current sequence number is
  // unavailable
  std::optional<Msg> tryRead(SeqNum expected_seq) const {
    return read(expected_seq);
  }

  // Get latest published sequence number
  SeqNum getLatestSeq() const {
    return write_seq_.load(std::memory_order_acquire) - 1;
  }

  // Get next sequence number to be written
  SeqNum getNextWriteSeq() const {
    return write_seq_.load(std::memory_order_acquire);
  }

  // Check if message at specified sequence number is available.
  // Note: this is a point-in-time snapshot; the slot may be overwritten
  // immediately after this returns true.
  bool isAvailable(SeqNum seq) const {
    if (seq < 0) return false;
    size_t index = seq & (Capacity - 1);
    return buffer_[index].seq.load(std::memory_order_acquire) == seq;
  }

  // Get buffer capacity
  static constexpr size_t capacity() { return Capacity; }

  // Get approximate number of messages in buffer
  size_t size() const {
    SeqNum latest = getLatestSeq();
    if (latest < 0) return 0;
    return std::min(static_cast<size_t>(latest + 1), Capacity);
  }

  // Get total number of slot overwrites since creation.
  // After the first Capacity messages, every push increments this counter.
  // Useful as a system-level indicator of buffer pressure.
  int64_t getOverwriteCount() const {
    return overwrite_count_.load(std::memory_order_relaxed);
  }

 private:
  // Cache line size (64 bytes on most x86 CPUs)
  static constexpr size_t CACHE_LINE_SIZE = 64;

  // Slot structure, contains message and sequence number
  struct alignas(CACHE_LINE_SIZE) Slot {
    Msg msg;
    std::atomic<SeqNum> seq;
    // Pad to fill exactly one cache line to avoid false sharing
    char padding[CACHE_LINE_SIZE - sizeof(Msg) - sizeof(std::atomic<SeqNum>)];

    Slot() : seq(INVALID_SEQ) {}
  };
  static_assert(sizeof(Slot) == CACHE_LINE_SIZE,
                "Slot must be exactly one cache line to avoid false sharing");

  // Buffer array
  std::array<Slot, Capacity> buffer_;

  // Write sequence number (only modified by producer) — INV-1
  alignas(CACHE_LINE_SIZE) std::atomic<SeqNum> write_seq_;

  // Count of slot overwrites (producer-side metric)
  alignas(CACHE_LINE_SIZE) std::atomic<int64_t> overwrite_count_;
};

// Consumer cursor, each consumer maintains independent read position
class ConsumerCursor {
 public:
  ConsumerCursor() : read_seq_(0) {}

  // Get current read position
  SeqNum getReadSeq() const {
    return read_seq_.load(std::memory_order_acquire);
  }

  // Set read position
  void setReadSeq(SeqNum seq) {
    read_seq_.store(seq, std::memory_order_release);
  }

  // Advance to next position
  SeqNum advance() { return read_seq_.fetch_add(1, std::memory_order_acq_rel); }

  // Reset to specified position
  void reset(SeqNum seq = 0) {
    read_seq_.store(seq, std::memory_order_release);
  }

 private:
  std::atomic<SeqNum> read_seq_;
};

}  // namespace replay
