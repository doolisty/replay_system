#include <chrono>
#include <cmath>
#include <memory>
#include <thread>
#include <vector>

#include "channel/FileChannel.hpp"
#include "client/MktDataClient.hpp"
#include "common/Message.hpp"
#include "common/RingBuffer.hpp"
#include "common/SpinLock.hpp"
#include "recorder/MktDataRecorder.hpp"
#include "server/MktDataServer.hpp"
#include "test_main.cpp"

using namespace replay;

// Test message structure
TEST(Consistency, MessageStructure) {
  Msg msg(100, 1234567890, 3.14159);

  ASSERT_EQ(msg.seq_num, 100);
  ASSERT_EQ(msg.timestamp_ns, 1234567890);
  ASSERT_NEAR(msg.payload, 3.14159, 1e-10);
  ASSERT_TRUE(msg.isValid());

  Msg invalid_msg;
  ASSERT_FALSE(invalid_msg.isValid());
}

// Test ring buffer basic operations
TEST(Consistency, RingBufferBasic) {
  RingBuffer<1024> buffer;

  // Write messages
  for (int i = 0; i < 100; ++i) {
    Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
    SeqNum seq = buffer.push(msg);
    ASSERT_EQ(seq, i);
  }

  // Read messages
  for (int i = 0; i < 100; ++i) {
    auto msg = buffer.read(i);
    ASSERT_TRUE(msg.has_value());
    ASSERT_EQ(msg->seq_num, i);
  }
}

// Test ring buffer concurrency
TEST(Consistency, RingBufferConcurrent) {
  RingBuffer<4096> buffer;
  const int MSG_COUNT = 1000;

  std::atomic<int64_t> producer_done{0};
  std::atomic<int64_t> consumer_sum{0};

  // Producer thread
  std::thread producer([&]() {
    for (int i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, 0, 1.0);
      buffer.push(msg);
    }
    producer_done = MSG_COUNT;
  });

  // Consumer thread
  std::thread consumer([&]() {
    int64_t count = 0;
    int64_t expected_seq = 0;

    while (count < MSG_COUNT) {
      auto msg = buffer.read(expected_seq);
      if (msg) {
        consumer_sum.fetch_add(1);
        expected_seq++;
        count++;
      } else {
        std::this_thread::yield();
      }
    }
  });

  producer.join();
  consumer.join();

  ASSERT_EQ(consumer_sum.load(), MSG_COUNT);
}

// Test spin lock
TEST(Consistency, SpinLock) {
  SpinLock lock;
  int counter = 0;
  const int ITERATIONS = 10000;

  std::vector<std::thread> threads;
  for (int t = 0; t < 4; ++t) {
    threads.emplace_back([&]() {
      for (int i = 0; i < ITERATIONS; ++i) {
        SpinLockGuard guard(lock);
        ++counter;
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }

  ASSERT_EQ(counter, 4 * ITERATIONS);
}

// Test file write and read
TEST(Consistency, FileIO) {
  const std::string TEST_FILE = "data/test_fileio.bin";
  const int MSG_COUNT = 100;

  // Write
  {
    FileWriteChannel writer(TEST_FILE);
    ASSERT_TRUE(writer.open());

    for (int i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i) * 1.5);
      ASSERT_TRUE(writer.write(msg));
    }

    writer.close();
  }

  // Read
  {
    FileChannel reader(TEST_FILE);
    ASSERT_TRUE(reader.open());
    ASSERT_EQ(reader.getMessageCount(), MSG_COUNT);

    for (int i = 0; i < MSG_COUNT; ++i) {
      auto msg = reader.readNext();
      ASSERT_TRUE(msg.has_value());
      ASSERT_EQ(msg->seq_num, i);
      ASSERT_NEAR(msg->payload, static_cast<double>(i) * 1.5, 1e-10);
    }

    reader.close();
  }
}

// Test sum consistency
TEST(Consistency, SumConsistency) {
  const int64_t MSG_COUNT = 5000;
  const std::string TEST_FILE = "data/test_sum.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(50000);  // High speed

  recorder.start();
  client.start();
  server.start();

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(200));

  client.stop();
  recorder.stop();

  // Verify sum
  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // Verify message count
  ASSERT_EQ(server.getSentCount(), MSG_COUNT);
}

// Test sequence number continuity
TEST(Consistency, SequenceNumbers) {
  RingBuffer<1024> buffer;

  // Write messages and verify sequence numbers
  for (int i = 0; i < 500; ++i) {
    Msg msg(0, 0, 0.0);
    SeqNum seq = buffer.push(msg);
    ASSERT_EQ(seq, i);
  }

  // Verify latest sequence number
  ASSERT_EQ(buffer.getLatestSeq(), 499);
}

#ifndef GTEST_FOUND

int main() {
  std::cout << "=== Consistency Test ===" << std::endl;

  RUN_TEST(Consistency, MessageStructure);
  RUN_TEST(Consistency, RingBufferBasic);
  RUN_TEST(Consistency, RingBufferConcurrent);
  RUN_TEST(Consistency, SpinLock);
  RUN_TEST(Consistency, FileIO);
  RUN_TEST(Consistency, SumConsistency);
  RUN_TEST(Consistency, SequenceNumbers);

  std::cout << "\nAll tests passed!" << std::endl;
  return 0;
}

#endif
