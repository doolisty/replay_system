#pragma once

#include <optional>
#include <string>

#include "common/Message.hpp"

namespace replay {

// Channel abstract interface
// Defines unified interface for message reading, supports different data
// sources (shared memory, files, etc.)
class IChannel {
 public:
  virtual ~IChannel() = default;

  // Open channel
  virtual bool open() = 0;

  // Close channel
  virtual void close() = 0;

  // Check if channel is open
  virtual bool isOpen() const = 0;

  // Read next message
  virtual std::optional<Msg> readNext() = 0;

  // Peek at next message without consuming
  virtual std::optional<Msg> peek() = 0;

  // Get channel name/description
  virtual std::string getName() const = 0;

  // Get latest available sequence number
  virtual SeqNum getLatestSeq() const = 0;

  // Seek to specified sequence number
  virtual bool seek(SeqNum seq) = 0;
};

// Writable channel interface
class IWritableChannel : public IChannel {
 public:
  // Write message
  virtual bool write(const Msg& msg) = 0;

  // Flush buffer to underlying storage
  virtual void flush() = 0;
};

}  // namespace replay
