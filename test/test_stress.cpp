#include <chrono>
#include <cmath>
#include <fstream>
#include <memory>
#include <thread>

#include "channel/FileChannel.hpp"
#include "client/MktDataClient.hpp"
#include "common/Message.hpp"
#include "common/RingBuffer.hpp"
#include "recorder/MktDataRecorder.hpp"
#include "replay/ReplayEngine.hpp"
#include "server/MktDataServer.hpp"
#include "test_main.cpp"

using namespace replay;

// ---------------------------------------------------------------------------
// Test 1: High-throughput stress test — verify correctness at speed.
//
// Sends a large number of messages at maximum rate (no throttle) and verifies
// that client sum == recorder sum at the end.
// ---------------------------------------------------------------------------
TEST(Stress, HighThroughput) {
  const int64_t MSG_COUNT = 100000;
  const std::string TEST_FILE = "data/test_stress_highthroughput.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(1000000);  // 1M msg/s — effectively no throttle

  recorder.start();
  client.start();
  server.start();

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(1000));

  client.stop();
  recorder.stop();

  // Both client and recorder should process all messages
  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);
  ASSERT_EQ(recorder.getRecordedCount(), MSG_COUNT);

  // Sums must match
  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // No gaps or overwrites at this buffer size
  ASSERT_EQ(client.getMetrics().seq_gap_count.load(), 0);
  ASSERT_EQ(recorder.getMetrics().seq_gap_count.load(), 0);
}

// ---------------------------------------------------------------------------
// Test 2: Recovery under high throughput — trigger fault during fast stream
// and verify correct sum after recovery.
// ---------------------------------------------------------------------------
TEST(Stress, RecoveryUnderLoad) {
  const int64_t MSG_COUNT = 50000;
  const std::string TEST_FILE = "data/test_stress_recovery.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(1000000);  // 1M msg/s — effectively no throttle

  recorder.start();
  client.start();
  server.start();

  // Wait until ~25% processed, then crash
  while (client.getLastSeq() < MSG_COUNT / 4 && server.isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  client.triggerFault(FaultType::CLIENT_CRASH);
  client.waitForRecovery();

  // Wait for completion
  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  client.stop();
  recorder.stop();

  // Client must have processed all messages and recovered correctly
  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);

  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // Exactly one recovery cycle
  ASSERT_EQ(client.getMetrics().recovery_count.load(), 1);
}

// ---------------------------------------------------------------------------
// Test 3: Multiple rapid faults — crash 5 times during a stream.
// ---------------------------------------------------------------------------
TEST(Stress, RapidMultipleFaults) {
  const int64_t MSG_COUNT = 20000;
  const std::string TEST_FILE = "data/test_stress_multifault.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(10000);

  recorder.start();
  client.start();
  server.start();

  const int FAULT_COUNT = 5;
  for (int i = 0; i < FAULT_COUNT; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    if (!server.isRunning()) break;
    client.triggerFault(FaultType::CLIENT_CRASH);
    client.waitForRecovery();
  }

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Wait for client to catch up
  int wait_count = 0;
  while (client.getProcessedCount() < MSG_COUNT && wait_count < 100) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    wait_count++;
  }

  client.stop();
  recorder.stop();

  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);

  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // Should have exactly FAULT_COUNT recoveries (or fewer if server finished
  // before all faults)
  ASSERT_GE(client.getMetrics().recovery_count.load(), 1);
}

// ---------------------------------------------------------------------------
// Test 4: readEx distinguishes NOT_READY from OVERWRITTEN.
//
// Use a tiny ring buffer (16 entries) and write enough to overwrite, then
// verify readEx returns OVERWRITTEN for old sequences.
// ---------------------------------------------------------------------------
TEST(Stress, RingBufferOverwriteDetection) {
  const size_t SMALL_CAPACITY = 16;
  RingBuffer<SMALL_CAPACITY> buffer;

  // Write 32 messages — first 16 will be overwritten
  for (int i = 0; i < 32; ++i) {
    Msg msg(i, 0, static_cast<double>(i));
    buffer.push(msg);
  }

  // Sequence 0 should be overwritten (slot reused by seq 16)
  auto result0 = buffer.readEx(0);
  ASSERT_TRUE(result0.status == ReadStatus::OVERWRITTEN);

  // Sequence 15 should also be overwritten
  auto result15 = buffer.readEx(15);
  ASSERT_TRUE(result15.status == ReadStatus::OVERWRITTEN);

  // Sequence 16 should be available (OK)
  auto result16 = buffer.readEx(16);
  ASSERT_TRUE(result16.status == ReadStatus::OK);
  ASSERT_EQ(result16.msg.seq_num, 16);

  // Sequence 31 should be available
  auto result31 = buffer.readEx(31);
  ASSERT_TRUE(result31.status == ReadStatus::OK);
  ASSERT_EQ(result31.msg.seq_num, 31);

  // Sequence 32 not yet written — NOT_READY
  auto result32 = buffer.readEx(32);
  ASSERT_TRUE(result32.status == ReadStatus::NOT_READY);

  // Overwrite count should be 16 (the second round of 16 writes each
  // overwrote a valid slot)
  ASSERT_EQ(buffer.getOverwriteCount(), 16);
}

// ---------------------------------------------------------------------------
// Test 5: File header integrity — verify first_seq, last_seq, flags.
// ---------------------------------------------------------------------------
TEST(Stress, FileHeaderIntegrity) {
  const std::string TEST_FILE = "data/test_stress_header.bin";
  const int MSG_COUNT = 200;

  // Write
  {
    FileWriteChannel writer(TEST_FILE);
    ASSERT_TRUE(writer.open());

    for (int i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i) * 0.5);
      ASSERT_TRUE(writer.write(msg));
    }

    writer.close();  // Should set FILE_FLAG_COMPLETE
  }

  // Read and verify header
  {
    FileChannel reader(TEST_FILE);
    ASSERT_TRUE(reader.open());

    ASSERT_EQ(reader.getMessageCount(), MSG_COUNT);
    ASSERT_EQ(reader.getFirstSeq(), 0);
    ASSERT_EQ(reader.getFileLastSeq(), MSG_COUNT - 1);
    ASSERT_TRUE(reader.wasCleanlyClose());

    // Read all messages and verify sequence continuity
    SeqNum prev_seq = INVALID_SEQ;
    for (int i = 0; i < MSG_COUNT; ++i) {
      auto msg = reader.readNext();
      ASSERT_TRUE(msg.has_value());
      ASSERT_EQ(msg->seq_num, i);
      if (prev_seq != INVALID_SEQ) {
        ASSERT_EQ(msg->seq_num, prev_seq + 1);
      }
      prev_seq = msg->seq_num;
    }

    reader.close();
  }
}

// ---------------------------------------------------------------------------
// Test 6: Incomplete file (simulate crash — no close()).
//
// Verify that the reader detects the missing FILE_FLAG_COMPLETE and still
// reads available data.
// ---------------------------------------------------------------------------
TEST(Stress, IncompleteFileRecovery) {
  const std::string TEST_FILE = "data/test_stress_incomplete.bin";
  const int MSG_COUNT = 50;

  // Write without closing — simulate crash
  {
    FileWriteChannel writer(TEST_FILE);
    ASSERT_TRUE(writer.open());

    for (int i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
      writer.write(msg);
    }

    // Flush to ensure data is on disk, but DON'T close (no FILE_FLAG_COMPLETE)
    writer.flush();

    // Manually close the underlying ofstream without calling close()
    // (FileWriteChannel destructor will call close(), so we need to use a
    // raw binary write approach instead)
  }
  // Destructor calls close() which sets the flag. We need a different approach
  // to truly test incomplete. Write the file manually:
  {
    std::ofstream file(TEST_FILE, std::ios::binary | std::ios::trunc);
    FileHeader header;
    header.msg_count = MSG_COUNT;
    header.first_seq = 0;
    header.last_seq = MSG_COUNT - 1;
    // Deliberately do NOT set FILE_FLAG_COMPLETE
    header.flags = 0;
    file.write(reinterpret_cast<const char*>(&header), sizeof(FileHeader));

    for (int i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
      file.write(reinterpret_cast<const char*>(&msg), sizeof(Msg));
    }
    file.close();
  }

  // Read — should succeed but wasCleanlyClose() should be false
  {
    FileChannel reader(TEST_FILE);
    ASSERT_TRUE(reader.open());
    ASSERT_EQ(reader.getMessageCount(), MSG_COUNT);
    ASSERT_FALSE(reader.wasCleanlyClose());

    // Data should still be readable
    for (int i = 0; i < MSG_COUNT; ++i) {
      auto msg = reader.readNext();
      ASSERT_TRUE(msg.has_value());
      ASSERT_EQ(msg->seq_num, i);
    }

    reader.close();
  }
}

// ---------------------------------------------------------------------------
// Test 7: ReplayEngine sequence validation.
//
// Create a file with an out-of-order message and verify that the replay engine
// detects the violation.
// ---------------------------------------------------------------------------
TEST(Stress, ReplaySequenceValidation) {
  const std::string TEST_FILE = "data/test_stress_seqvalidation.bin";

  // Write file with intentional sequence disorder
  {
    std::ofstream file(TEST_FILE, std::ios::binary | std::ios::trunc);
    FileHeader header;
    header.msg_count = 5;
    header.first_seq = 0;
    header.last_seq = 4;
    header.flags = FILE_FLAG_COMPLETE;
    file.write(reinterpret_cast<const char*>(&header), sizeof(FileHeader));

    // Normal: 0, 1, 2 then out-of-order: 1 (duplicate), then 4
    int64_t seqs[] = {0, 1, 2, 1, 4};
    for (int i = 0; i < 5; ++i) {
      Msg msg(seqs[i], getCurrentTimestampNs(), 1.0);
      file.write(reinterpret_cast<const char*>(&msg), sizeof(Msg));
    }
    file.close();
  }

  // Replay and check violation detection
  {
    ReplayEngine engine(TEST_FILE);
    ASSERT_TRUE(engine.open());
    ASSERT_TRUE(engine.wasFileCleanlyClose());

    // Read all 5 messages
    for (int i = 0; i < 5; ++i) {
      auto msg = engine.nextMessage();
      ASSERT_TRUE(msg.has_value());
    }

    // Should have detected violations (seq 1 after seq 2, and seq 4 after 1)
    ASSERT_GT(engine.getSeqViolationCount(), 0);

    engine.close();
  }
}

// ---------------------------------------------------------------------------
// Test 8: Replay-to-live boundary continuity check.
//
// Verify that after recovery, the client's processed messages form a
// contiguous sequence from 0..N with no duplicates.
// ---------------------------------------------------------------------------
TEST(Stress, ReplayLiveBoundaryContinuity) {
  const int64_t MSG_COUNT = 5000;
  const std::string TEST_FILE = "data/test_stress_boundary.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  // Use deterministic payload = 1.0 for easy sum verification
  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(10000);
  server.setMessageGenerator([]() { return 1.0; });

  recorder.start();
  client.start();
  server.start();

  // Fault at ~40%
  while (client.getLastSeq() < MSG_COUNT * 2 / 5 && server.isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  client.triggerFault(FaultType::CLIENT_CRASH);
  client.waitForRecovery();

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Wait for client to finish
  int wait_count = 0;
  while (client.getProcessedCount() < MSG_COUNT && wait_count < 100) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    wait_count++;
  }

  client.stop();
  recorder.stop();

  // With payload=1.0, expected sum = MSG_COUNT * 1.0
  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);
  ASSERT_NEAR(client.getSum(), static_cast<double>(MSG_COUNT), 1e-6);

  // No gaps should have occurred
  ASSERT_EQ(client.getMetrics().seq_gap_count.load(), 0);
}

// ---------------------------------------------------------------------------
// Test 9: Metrics observability — verify counters are accessible and correct.
// ---------------------------------------------------------------------------
TEST(Stress, MetricsObservability) {
  const int64_t MSG_COUNT = 1000;
  const std::string TEST_FILE = "data/test_stress_metrics.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(10000);

  recorder.start();
  client.start();
  server.start();

  // Trigger a crash at ~50%
  while (client.getLastSeq() < MSG_COUNT / 2 && server.isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }
  client.triggerFault(FaultType::CLIENT_CRASH);
  client.waitForRecovery();

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(300));

  client.stop();
  recorder.stop();

  // Verify metrics accessibility
  const auto& cm = client.getMetrics();
  ASSERT_EQ(cm.recovery_count.load(), 1);
  ASSERT_EQ(cm.overwrite_count.load(), 0);

  const auto& rm = recorder.getMetrics();
  ASSERT_EQ(rm.overwrite_count.load(), 0);

  // Buffer overwrite counter should be zero for large buffer + small msg count
  ASSERT_EQ(buffer->getOverwriteCount(), 0);
}

#ifndef GTEST_FOUND

int main() {
  std::cout << "=== Stress & Regression Test ===" << std::endl;

  RUN_TEST(Stress, RingBufferOverwriteDetection);
  RUN_TEST(Stress, FileHeaderIntegrity);
  RUN_TEST(Stress, IncompleteFileRecovery);
  RUN_TEST(Stress, ReplaySequenceValidation);
  RUN_TEST(Stress, HighThroughput);
  RUN_TEST(Stress, RecoveryUnderLoad);
  RUN_TEST(Stress, RapidMultipleFaults);
  RUN_TEST(Stress, ReplayLiveBoundaryContinuity);
  RUN_TEST(Stress, MetricsObservability);

  std::cout << "\nAll stress tests passed!" << std::endl;
  return 0;
}

#endif
