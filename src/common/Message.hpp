#pragma once

#include <cstring>

#include "Types.hpp"

namespace replay {

// Message structure: 24 bytes, cache line alignment friendly
struct alignas(8) Msg {
  SeqNum seq_num;        // Sequence number (8 bytes)
  int64_t timestamp_ns;  // Nanosecond timestamp (8 bytes)
  double payload;        // Data payload (8 bytes)

  constexpr Msg() noexcept
      : seq_num(INVALID_SEQ), timestamp_ns(0), payload(0.0) {}

  constexpr Msg(SeqNum seq, int64_t ts, double data) noexcept
      : seq_num(seq), timestamp_ns(ts), payload(data) {}

  // Check if message is valid
  [[nodiscard]] constexpr bool isValid() const noexcept {
    return seq_num != INVALID_SEQ;
  }

  // Reset message
  constexpr void reset() noexcept {
    seq_num = INVALID_SEQ;
    timestamp_ns = 0;
    payload = 0.0;
  }

  // Comparison operators (by sequence number)
  constexpr bool operator<(const Msg& other) const noexcept {
    return seq_num < other.seq_num;
  }

  constexpr bool operator==(const Msg& other) const noexcept {
    return seq_num == other.seq_num && timestamp_ns == other.timestamp_ns &&
           payload == other.payload;
  }
};

static_assert(sizeof(Msg) == 24, "Msg size must be 24 bytes");

// File header structure: 64 bytes (expanded for integrity tracking)
//
// Invariants maintained by the recorder:
//   - first_seq <= last_seq when msg_count > 0
//   - last_seq - first_seq + 1 == msg_count (no gaps in recording)
//   - FILE_FLAG_COMPLETE is set only after a clean close
struct alignas(8) FileHeader {
  uint32_t magic;       // Magic number (4 bytes) — FILE_MAGIC
  uint16_t version;     // Version number (2 bytes) — FILE_VERSION
  uint16_t flags;       // Flags (2 bytes) — see FILE_FLAG_*
  uint32_t date;        // Date YYYYMMDD (4 bytes)
  uint32_t reserved1;   // Reserved (4 bytes)
  int64_t msg_count;    // Message count (8 bytes)
  int64_t first_seq;    // First sequence number in file (8 bytes), INVALID_SEQ if empty
  int64_t last_seq;     // Last sequence number in file (8 bytes), INVALID_SEQ if empty
  int64_t reserved2[3]; // Reserved for future use (24 bytes)

  constexpr FileHeader() noexcept
      : magic(FILE_MAGIC),
        version(FILE_VERSION),
        flags(0),
        date(0),
        reserved1(0),
        msg_count(0),
        first_seq(INVALID_SEQ),
        last_seq(INVALID_SEQ),
        reserved2{0, 0, 0} {}

  [[nodiscard]] constexpr bool isValid() const noexcept {
    return magic == FILE_MAGIC && version == FILE_VERSION;
  }

  // Check structural consistency of header fields
  [[nodiscard]] constexpr bool isConsistent() const noexcept {
    if (!isValid()) return false;
    if (msg_count < 0) return false;
    if (msg_count == 0) {
      return first_seq == INVALID_SEQ && last_seq == INVALID_SEQ;
    }
    // Non-empty: first_seq and last_seq must be valid and consistent
    if (first_seq < 0 || last_seq < 0) return false;
    if (first_seq > last_seq) return false;
    if (last_seq - first_seq + 1 != msg_count) return false;
    return true;
  }

  // Check whether the file was properly closed
  [[nodiscard]] constexpr bool isComplete() const noexcept {
    return (flags & FILE_FLAG_COMPLETE) != 0;
  }
};

static_assert(sizeof(FileHeader) == 64, "FileHeader size must be 64 bytes");

}  // namespace replay
