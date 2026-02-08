#include <chrono>
#include <cmath>
#include <memory>
#include <thread>

#include "client/MktDataClient.hpp"
#include "common/RingBuffer.hpp"
#include "recorder/MktDataRecorder.hpp"
#include "server/MktDataServer.hpp"
#include "test_main.cpp"

using namespace replay;

// Test recovery after client crash
TEST(Recovery, ClientCrashRecovery) {
  const int64_t MSG_COUNT = 1000;
  const std::string TEST_FILE = "data/test_recovery.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(10000);  // High-speed sending

  recorder.start();
  client.start();
  server.start();

  // Wait for half of messages to be processed
  while (client.getLastSeq() < MSG_COUNT / 2) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Trigger fault
  client.triggerFault(FaultType::CLIENT_CRASH);

  // Wait for recovery
  client.waitForRecovery();

  // Wait for completion
  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Wait for client to process all messages
  while (client.getLastSeq() < MSG_COUNT - 1) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }
  std::this_thread::sleep_for(
      std::chrono::milliseconds(50));  // Ensure processing is complete

  client.stop();
  recorder.stop();

  // Verify results
  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);
}

// Test immediate fault on startup
TEST(Recovery, ImmediateFault) {
  const int64_t MSG_COUNT = 500;
  const std::string TEST_FILE = "data/test_immediate.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(10000);

  recorder.start();
  client.start();
  server.start();

  // Trigger fault immediately
  std::this_thread::sleep_for(std::chrono::milliseconds(10));
  client.triggerFault(FaultType::CLIENT_CRASH);

  client.waitForRecovery();
  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  client.stop();
  recorder.stop();

  // Basic verification: processing complete
  ASSERT_GT(client.getProcessedCount(), 0);
}

// Test multiple consecutive faults
TEST(Recovery, MultipleFaults) {
  const int64_t MSG_COUNT = 2000;
  const std::string TEST_FILE = "data/test_multi.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(5000);

  recorder.start();
  client.start();
  server.start();

  // Multiple faults
  for (int i = 0; i < 3; ++i) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    if (!server.isRunning()) break;
    client.triggerFault(FaultType::CLIENT_CRASH);
    client.waitForRecovery();
  }

  server.waitForComplete();
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  client.stop();
  recorder.stop();

  ASSERT_GT(client.getProcessedCount(), 0);
}

#ifndef GTEST_FOUND

int main() {
  std::cout << "=== Fault Recovery Test ===" << std::endl;

  RUN_TEST(Recovery, ClientCrashRecovery);
  RUN_TEST(Recovery, ImmediateFault);
  RUN_TEST(Recovery, MultipleFaults);

  std::cout << "\nAll tests passed!" << std::endl;
  return 0;
}

#endif
