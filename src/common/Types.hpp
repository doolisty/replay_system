#pragma once

#include <chrono>
#include <cstdint>

namespace replay {

// Time type definitions
using Clock = std::chrono::high_resolution_clock;
using TimePoint = Clock::time_point;
using Duration = Clock::duration;
using Nanoseconds = std::chrono::nanoseconds;

// Sequence number type
using SeqNum = int64_t;

// Invalid sequence number
constexpr SeqNum INVALID_SEQ = -1;

// Catch-up threshold: maximum gap between replay sequence number and live
// sequence number
constexpr int64_t CATCHUP_THRESHOLD = 100;

// Default ring buffer size
constexpr size_t DEFAULT_RING_BUFFER_SIZE = 1024 * 1024;  // 1M entries

// Disk write batch size
constexpr size_t DISK_BATCH_SIZE = 1024;

// File magic number
constexpr uint32_t FILE_MAGIC = 0x4D4B5444;  // "MKTD"

// File version (bumped to 2 for extended header with integrity fields)
constexpr uint16_t FILE_VERSION = 2;

// File flags (stored in FileHeader.flags)
constexpr uint16_t FILE_FLAG_COMPLETE = 0x0001;  // File was properly closed

// RingBuffer read status — distinguishes "not yet published" from "overwritten"
enum class ReadStatus {
  OK,          // Message read successfully
  NOT_READY,   // Message not yet published by producer
  OVERWRITTEN  // Message was overwritten (consumer too slow)
};

// Fault type
enum class FaultType {
  CLIENT_CRASH,   // Client crash (reset accumulated value)
  MESSAGE_LOSS,   // Message loss (skip some messages)
  TEMPORARY_HANG  // Temporary hang (block for a period)
};

// Client state
enum class ClientState {
  NORMAL,      // Normal operation
  FAULTED,     // Fault state
  REPLAYING,   // Replaying
  CATCHING_UP  // Catching up
};

// Get current nanosecond timestamp
inline int64_t getCurrentTimestampNs() {
  return std::chrono::duration_cast<Nanoseconds>(
             Clock::now().time_since_epoch())
      .count();
}

}  // namespace replay
