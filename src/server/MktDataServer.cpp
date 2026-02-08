#include "MktDataServer.hpp"

#include <chrono>

#include "common/Logging.hpp"

namespace replay {

MktDataServer::MktDataServer(RingBufferType& buffer)
    : buffer_(buffer),
      running_(false),
      stop_requested_(false),
      message_count_(10000),
      message_rate_(1000),
      sent_count_(0),
      generator_(nullptr),
      rng_(std::random_device{}()),
      dist_(0.0, 100.0) {}

MktDataServer::~MktDataServer() { stop(); }

void MktDataServer::start() {
  if (running_) {
    LOG_WARNING(replay::logger(),
                "MktDataServer already running, ignoring start {}", "");
    return;
  }

  stop_requested_ = false;
  sent_count_ = 0;
  running_ = true;

  LOG_INFO(replay::logger(), "MktDataServer start: messages={}, rate={}",
           message_count_, message_rate_);

  thread_ = std::thread(&MktDataServer::run, this);
}

void MktDataServer::stop() {
  stop_requested_ = true;

  if (thread_.joinable()) {
    thread_.join();
  }

  running_ = false;
  LOG_INFO(replay::logger(), "MktDataServer stopped: sent={}", getSentCount());
}

void MktDataServer::waitForComplete() {
  if (thread_.joinable()) {
    thread_.join();
  }
}

bool MktDataServer::isRunning() const { return running_; }

void MktDataServer::setMessageCount(int64_t count) { message_count_ = count; }

void MktDataServer::setMessageRate(int64_t rate_per_second) {
  message_rate_ = rate_per_second;
}

void MktDataServer::setMessageGenerator(MessageGenerator generator) {
  generator_ = std::move(generator);
}

void MktDataServer::setCpuCore(int core_id) { cpu_core_ = core_id; }

int64_t MktDataServer::getSentCount() const {
  return sent_count_.load(std::memory_order_acquire);
}

SeqNum MktDataServer::getLatestSeq() const { return buffer_.getLatestSeq(); }

void MktDataServer::run() {
  setCpuAffinity(cpu_core_, "MktDataServer");

  using namespace std::chrono;

  // Calculate interval time for each message
  auto interval_ns = nanoseconds(1000000000 / message_rate_);
  auto start_time = high_resolution_clock::now();

  for (int64_t i = 0; i < message_count_ && !stop_requested_; ++i) {
    // Generate message
    double payload = generatePayload();
    int64_t timestamp = getCurrentTimestampNs();

    Msg msg(INVALID_SEQ, timestamp,
            payload);  // Sequence number assigned by RingBuffer

    // Write to buffer
    buffer_.push(msg);
    sent_count_.fetch_add(1, std::memory_order_release);

    // Rate control
    if (message_rate_ > 0) {
      auto expected_time = start_time + interval_ns * (i + 1);
      auto now = high_resolution_clock::now();

      if (now < expected_time) {
        std::this_thread::sleep_until(expected_time);
      }
    }
  }

  running_ = false;
  LOG_INFO(replay::logger(), "MktDataServer completed: sent={}",
           getSentCount());
}

double MktDataServer::generatePayload() {
  if (generator_) {
    return generator_();
  }
  return dist_(rng_);
}

}  // namespace replay
