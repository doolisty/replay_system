#include <algorithm>
#include <chrono>
#include <cmath>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <memory>
#include <numeric>
#include <thread>
#include <vector>

#include "channel/FileChannel.hpp"
#include "client/MktDataClient.hpp"
#include "common/Message.hpp"
#include "common/RingBuffer.hpp"
#include "common/SpinLock.hpp"
#include "recorder/MktDataRecorder.hpp"
#include "replay/ReplayEngine.hpp"
#include "server/MktDataServer.hpp"
#include "test_main.cpp"

using namespace replay;
using namespace std::chrono;

// ---------------------------------------------------------------------------
// Helper: high-resolution timer
// ---------------------------------------------------------------------------
struct BenchTimer {
  high_resolution_clock::time_point t0;

  void start() { t0 = high_resolution_clock::now(); }

  double elapsed_ns() const {
    auto t1 = high_resolution_clock::now();
    return static_cast<double>(
        duration_cast<nanoseconds>(t1 - t0).count());
  }

  double elapsed_us() const { return elapsed_ns() / 1000.0; }
  double elapsed_ms() const { return elapsed_ns() / 1e6; }
  double elapsed_s() const { return elapsed_ns() / 1e9; }
};

// ---------------------------------------------------------------------------
// Helper: compute statistics from a vector of latency samples (in ns)
// ---------------------------------------------------------------------------
struct LatencyStats {
  double min_ns;
  double max_ns;
  double mean_ns;
  double median_ns;
  double p50_ns;
  double p90_ns;
  double p99_ns;
  double p999_ns;
  size_t count;
};

static LatencyStats computeStats(std::vector<double>& samples) {
  LatencyStats stats{};
  if (samples.empty()) return stats;

  std::sort(samples.begin(), samples.end());
  stats.count = samples.size();
  stats.min_ns = samples.front();
  stats.max_ns = samples.back();
  stats.mean_ns =
      std::accumulate(samples.begin(), samples.end(), 0.0) /
      static_cast<double>(stats.count);

  auto percentile = [&](double p) -> double {
    size_t idx =
        static_cast<size_t>(p / 100.0 * static_cast<double>(stats.count - 1));
    return samples[idx];
  };

  stats.median_ns = percentile(50);
  stats.p50_ns = percentile(50);
  stats.p90_ns = percentile(90);
  stats.p99_ns = percentile(99);
  stats.p999_ns = percentile(99.9);

  return stats;
}

static void printStats(const std::string& label, const LatencyStats& s) {
  std::cout << "  [" << label << "]  count=" << s.count << std::endl;
  std::cout << "    min=" << std::fixed << std::setprecision(1) << s.min_ns
            << " ns, mean=" << s.mean_ns << " ns, median=" << s.median_ns
            << " ns" << std::endl;
  std::cout << "    p90=" << s.p90_ns << " ns, p99=" << s.p99_ns
            << " ns, p99.9=" << s.p999_ns << " ns, max=" << s.max_ns << " ns"
            << std::endl;
}

// ===========================================================================
// Benchmark 1: RingBuffer single-threaded push latency
//
// Measures the per-message cost of writing to the ring buffer from a single
// producer thread.  Target: < 100 ns per push.
// ===========================================================================
TEST(Benchmark, RingBufferPushLatency) {
  const size_t WARMUP = 10000;
  const size_t ITERATIONS = 1000000;

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

  // Warm up — fill caches, trigger any lazy initialization
  for (size_t i = 0; i < WARMUP; ++i) {
    Msg msg(static_cast<SeqNum>(i), getCurrentTimestampNs(), 1.0);
    buffer->push(msg);
  }

  // Re-create to start clean
  auto buffer2 = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  std::vector<double> latencies;
  latencies.reserve(ITERATIONS);

  for (size_t i = 0; i < ITERATIONS; ++i) {
    Msg msg(static_cast<SeqNum>(i), getCurrentTimestampNs(), 1.0);
    auto t0 = high_resolution_clock::now();
    buffer2->push(msg);
    auto t1 = high_resolution_clock::now();
    latencies.push_back(
        static_cast<double>(duration_cast<nanoseconds>(t1 - t0).count()));
  }

  auto stats = computeStats(latencies);

  std::cout << "\n=== Benchmark: RingBuffer Push Latency ===" << std::endl;
  printStats("push", stats);

  // Assert: median push latency should be under 500 ns (generous bound)
  ASSERT_LT(stats.median_ns, 500.0);
}

// ===========================================================================
// Benchmark 2: RingBuffer single-threaded read (readEx) latency
//
// Measures the per-message cost of reading from the ring buffer.
// Target: < 100 ns per read.
// ===========================================================================
TEST(Benchmark, RingBufferReadLatency) {
  const size_t COUNT = 1000000;

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

  // Pre-fill the buffer
  for (size_t i = 0; i < COUNT; ++i) {
    Msg msg(static_cast<SeqNum>(i), getCurrentTimestampNs(),
            static_cast<double>(i));
    buffer->push(msg);
  }

  std::vector<double> latencies;
  latencies.reserve(COUNT);

  for (size_t i = 0; i < COUNT; ++i) {
    auto t0 = high_resolution_clock::now();
    auto result = buffer->readEx(static_cast<SeqNum>(i));
    auto t1 = high_resolution_clock::now();
    (void)result;
    latencies.push_back(
        static_cast<double>(duration_cast<nanoseconds>(t1 - t0).count()));
  }

  auto stats = computeStats(latencies);

  std::cout << "\n=== Benchmark: RingBuffer Read Latency ===" << std::endl;
  printStats("readEx", stats);

  ASSERT_LT(stats.median_ns, 500.0);
}

// ===========================================================================
// Benchmark 3: RingBuffer producer-consumer throughput (SPSC)
//
// One producer thread pushes as fast as possible while one consumer reads.
// Measures end-to-end throughput in msg/s.
// Target: > 10M msg/s.
// ===========================================================================
TEST(Benchmark, RingBufferSPSCThroughput) {
  const int64_t MSG_COUNT = 5000000;

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

  std::atomic<bool> consumer_done{false};
  std::atomic<int64_t> consumer_count{0};

  // Consumer thread
  std::thread consumer([&]() {
    SeqNum seq = 0;
    while (seq < MSG_COUNT) {
      auto result = buffer->readEx(seq);
      if (result.status == ReadStatus::OK) {
        ++seq;
      }
      // Spin on NOT_READY — tight loop
    }
    consumer_count.store(seq, std::memory_order_relaxed);
    consumer_done.store(true, std::memory_order_release);
  });

  // Producer: push as fast as possible
  BenchTimer timer;
  timer.start();

  for (int64_t i = 0; i < MSG_COUNT; ++i) {
    Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
    buffer->push(msg);
  }

  // Wait for consumer to finish
  consumer.join();

  double elapsed = timer.elapsed_s();
  double throughput = static_cast<double>(MSG_COUNT) / elapsed;

  std::cout << "\n=== Benchmark: RingBuffer SPSC Throughput ===" << std::endl;
  std::cout << "  Messages:   " << MSG_COUNT << std::endl;
  std::cout << "  Elapsed:    " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
            << throughput / 1e6 << " M msg/s" << std::endl;

  // Assert: at least 1M msg/s (conservative for CI environments)
  ASSERT_GT(throughput, 1e6);
}

// ===========================================================================
// Benchmark 4: RingBuffer SPMC throughput (1 producer, 4 consumers)
//
// Measures throughput when multiple consumers compete for reads.
// ===========================================================================
TEST(Benchmark, RingBufferSPMCThroughput) {
  const int64_t MSG_COUNT = 2000000;
  const int NUM_CONSUMERS = 4;

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

  std::vector<std::thread> consumers;
  std::vector<int64_t> consumer_counts(NUM_CONSUMERS, 0);

  // Launch consumer threads
  for (int c = 0; c < NUM_CONSUMERS; ++c) {
    consumers.emplace_back([&, c]() {
      SeqNum seq = 0;
      while (seq < MSG_COUNT) {
        auto result = buffer->readEx(seq);
        if (result.status == ReadStatus::OK) {
          ++seq;
        }
      }
      consumer_counts[c] = seq;
    });
  }

  BenchTimer timer;
  timer.start();

  // Producer: push as fast as possible
  for (int64_t i = 0; i < MSG_COUNT; ++i) {
    Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
    buffer->push(msg);
  }

  for (auto& t : consumers) {
    t.join();
  }

  double elapsed = timer.elapsed_s();
  double throughput = static_cast<double>(MSG_COUNT) / elapsed;

  std::cout << "\n=== Benchmark: RingBuffer SPMC Throughput (1P/"
            << NUM_CONSUMERS << "C) ===" << std::endl;
  std::cout << "  Messages:   " << MSG_COUNT << std::endl;
  std::cout << "  Elapsed:    " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
            << throughput / 1e6 << " M msg/s (producer rate)" << std::endl;

  // All consumers must have read all messages
  for (int c = 0; c < NUM_CONSUMERS; ++c) {
    ASSERT_EQ(consumer_counts[c], MSG_COUNT);
  }

  ASSERT_GT(throughput, 0.5e6);
}

// ===========================================================================
// Benchmark 5: RingBuffer batch push vs single push
//
// Compare pushBatch throughput against individual push calls.
// ===========================================================================
TEST(Benchmark, RingBufferBatchPush) {
  const size_t TOTAL = 1000000;
  const size_t BATCH_SIZE = 64;

  // Single push
  {
    auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

    BenchTimer timer;
    timer.start();

    for (size_t i = 0; i < TOTAL; ++i) {
      Msg msg(static_cast<SeqNum>(i), 0, 1.0);
      buffer->push(msg);
    }

    double single_elapsed = timer.elapsed_ms();

    std::cout << "\n=== Benchmark: Batch Push vs Single Push ===" << std::endl;
    std::cout << "  Single push: " << TOTAL << " msgs in " << std::fixed
              << std::setprecision(2) << single_elapsed << " ms ("
              << static_cast<double>(TOTAL) / single_elapsed * 1000.0 / 1e6
              << " M msg/s)" << std::endl;
  }

  // Batch push
  {
    auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

    std::vector<Msg> batch(BATCH_SIZE);
    BenchTimer timer;
    timer.start();

    for (size_t i = 0; i < TOTAL; i += BATCH_SIZE) {
      size_t count = std::min(BATCH_SIZE, TOTAL - i);
      for (size_t j = 0; j < count; ++j) {
        batch[j] =
            Msg(static_cast<SeqNum>(i + j), 0, 1.0);
      }
      buffer->pushBatch(
          std::span<const Msg>(batch.data(), count));
    }

    double batch_elapsed = timer.elapsed_ms();

    std::cout << "  Batch push (batch=" << BATCH_SIZE << "): " << TOTAL
              << " msgs in " << std::fixed << std::setprecision(2)
              << batch_elapsed << " ms ("
              << static_cast<double>(TOTAL) / batch_elapsed * 1000.0 / 1e6
              << " M msg/s)" << std::endl;
  }

  // Just pass — this is an informational benchmark
  ASSERT_TRUE(true);
}

// ===========================================================================
// Benchmark 6: File I/O write throughput
//
// Measures raw FileWriteChannel write speed.
// Target: > 100 MB/s (sizeof(Msg)=24 bytes).
// ===========================================================================
TEST(Benchmark, FileWriteThroughput) {
  const int64_t MSG_COUNT = 2000000;
  const std::string TEST_FILE = "data/bench_file_write.bin";

  FileWriteChannel writer(TEST_FILE);
  ASSERT_TRUE(writer.open());

  BenchTimer timer;
  timer.start();

  for (int64_t i = 0; i < MSG_COUNT; ++i) {
    Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i) * 0.1);
    writer.write(msg);
  }
  writer.close();

  double elapsed = timer.elapsed_s();
  double bytes = static_cast<double>(MSG_COUNT) * sizeof(Msg);
  double mb_per_s = bytes / elapsed / (1024.0 * 1024.0);
  double msg_per_s = static_cast<double>(MSG_COUNT) / elapsed;

  std::cout << "\n=== Benchmark: File Write Throughput ===" << std::endl;
  std::cout << "  Messages:   " << MSG_COUNT << " (" << std::fixed
            << std::setprecision(1) << bytes / (1024.0 * 1024.0) << " MB)"
            << std::endl;
  std::cout << "  Elapsed:    " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput: " << std::fixed << std::setprecision(1)
            << mb_per_s << " MB/s (" << std::setprecision(2)
            << msg_per_s / 1e6 << " M msg/s)" << std::endl;

  // Assert: at least 50 MB/s (conservative for CI)
  ASSERT_GT(mb_per_s, 50.0);
}

// ===========================================================================
// Benchmark 7: File I/O read throughput
//
// Measures raw FileChannel sequential read speed.
// Target: > 100 MB/s.
// ===========================================================================
TEST(Benchmark, FileReadThroughput) {
  const int64_t MSG_COUNT = 2000000;
  const std::string TEST_FILE = "data/bench_file_read.bin";

  // Prepare test file
  {
    FileWriteChannel writer(TEST_FILE);
    ASSERT_TRUE(writer.open());
    for (int64_t i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
      writer.write(msg);
    }
    writer.close();
  }

  // Benchmark read
  FileChannel reader(TEST_FILE);
  ASSERT_TRUE(reader.open());

  BenchTimer timer;
  timer.start();

  int64_t count = 0;
  while (auto msg = reader.readNext()) {
    ++count;
    (void)msg;
  }

  double elapsed = timer.elapsed_s();
  double bytes = static_cast<double>(count) * sizeof(Msg);
  double mb_per_s = bytes / elapsed / (1024.0 * 1024.0);
  double msg_per_s = static_cast<double>(count) / elapsed;

  reader.close();

  std::cout << "\n=== Benchmark: File Read Throughput ===" << std::endl;
  std::cout << "  Messages:   " << count << " (" << std::fixed
            << std::setprecision(1) << bytes / (1024.0 * 1024.0) << " MB)"
            << std::endl;
  std::cout << "  Elapsed:    " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput: " << std::fixed << std::setprecision(1)
            << mb_per_s << " MB/s (" << std::setprecision(2)
            << msg_per_s / 1e6 << " M msg/s)" << std::endl;

  ASSERT_EQ(count, MSG_COUNT);
  ASSERT_GT(mb_per_s, 50.0);
}

// ===========================================================================
// Benchmark 8: ReplayEngine throughput
//
// Measures replay speed including sequence validation overhead.
// Target: > 1M msg/s.
// ===========================================================================
TEST(Benchmark, ReplayEngineThroughput) {
  const int64_t MSG_COUNT = 2000000;
  const std::string TEST_FILE = "data/bench_replay.bin";

  // Prepare test file
  {
    FileWriteChannel writer(TEST_FILE);
    ASSERT_TRUE(writer.open());
    for (int64_t i = 0; i < MSG_COUNT; ++i) {
      Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
      writer.write(msg);
    }
    writer.close();
  }

  // Benchmark replay
  ReplayEngine engine(TEST_FILE);
  ASSERT_TRUE(engine.open());

  BenchTimer timer;
  timer.start();

  int64_t count = 0;
  while (auto msg = engine.nextMessage()) {
    ++count;
    (void)msg;
  }

  double elapsed = timer.elapsed_s();
  double msg_per_s = static_cast<double>(count) / elapsed;

  engine.close();

  std::cout << "\n=== Benchmark: ReplayEngine Throughput ===" << std::endl;
  std::cout << "  Messages:   " << count << std::endl;
  std::cout << "  Elapsed:    " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput: " << std::fixed << std::setprecision(2)
            << msg_per_s / 1e6 << " M msg/s" << std::endl;
  std::cout << "  Seq violations: " << engine.getSeqViolationCount()
            << std::endl;

  ASSERT_EQ(count, MSG_COUNT);
  ASSERT_EQ(engine.getSeqViolationCount(), 0);
  ASSERT_GT(msg_per_s, 1e6);
}

// ===========================================================================
// Benchmark 9: SpinLock contention — throughput under N threads
//
// Measures how many lock/unlock cycles per second can be achieved with
// increasing thread counts (1, 2, 4).
// ===========================================================================
TEST(Benchmark, SpinLockContention) {
  const int64_t OPS_PER_THREAD = 1000000;

  std::cout << "\n=== Benchmark: SpinLock Contention ===" << std::endl;

  for (int num_threads : {1, 2, 4}) {
    SpinLock lock;
    int64_t shared_counter = 0;

    std::vector<std::thread> threads;

    BenchTimer timer;
    timer.start();

    for (int t = 0; t < num_threads; ++t) {
      threads.emplace_back([&]() {
        for (int64_t i = 0; i < OPS_PER_THREAD; ++i) {
          SpinLockGuard guard(lock);
          shared_counter++;
        }
      });
    }

    for (auto& t : threads) {
      t.join();
    }

    double elapsed = timer.elapsed_s();
    int64_t total_ops = static_cast<int64_t>(num_threads) * OPS_PER_THREAD;
    double ops_per_s = static_cast<double>(total_ops) / elapsed;

    std::cout << "  " << num_threads
              << " thread(s): " << std::fixed << std::setprecision(2)
              << ops_per_s / 1e6 << " M ops/s, elapsed="
              << std::setprecision(1) << elapsed * 1000.0 << " ms"
              << std::endl;

    ASSERT_EQ(shared_counter, total_ops);
  }
}

// ===========================================================================
// Benchmark 10: End-to-end pipeline latency
//
// Measures latency from producer push to consumer receive using timestamp
// embedded in messages. This captures the realistic end-to-end path:
// Server → RingBuffer → Client.
// ===========================================================================
TEST(Benchmark, EndToEndLatency) {
  const int64_t MSG_COUNT = 500000;
  const std::string TEST_FILE = "data/bench_e2e.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();

  // We will measure latency by comparing the timestamp in the message
  // (set at push time) with the time at which the consumer reads it.
  std::vector<double> latencies;
  latencies.resize(MSG_COUNT, 0.0);

  std::atomic<bool> consumer_done{false};

  // Consumer thread — reads as fast as possible and records latency
  std::thread consumer([&]() {
    SeqNum seq = 0;
    while (seq < MSG_COUNT) {
      auto result = buffer->readEx(seq);
      if (result.status == ReadStatus::OK) {
        int64_t now = getCurrentTimestampNs();
        latencies[static_cast<size_t>(seq)] =
            static_cast<double>(now - result.msg.timestamp_ns);
        ++seq;
      }
    }
    consumer_done.store(true, std::memory_order_release);
  });

  // Producer — push with current timestamp
  for (int64_t i = 0; i < MSG_COUNT; ++i) {
    Msg msg(i, getCurrentTimestampNs(), static_cast<double>(i));
    buffer->push(msg);
  }

  consumer.join();

  auto stats = computeStats(latencies);

  std::cout << "\n=== Benchmark: End-to-End Latency ===" << std::endl;
  std::cout << "  Messages: " << MSG_COUNT << std::endl;
  printStats("e2e", stats);

  // Assert: median latency should be under 10 μs in most environments
  ASSERT_LT(stats.median_ns, 10000.0);
}

// ===========================================================================
// Benchmark 11: Full system throughput (Server + Client + Recorder)
//
// Runs the complete pipeline at maximum rate and measures wall-clock time.
// Verifies correctness (sum match) alongside performance.
// ===========================================================================
TEST(Benchmark, FullSystemThroughput) {
  const int64_t MSG_COUNT = 1000000;
  const std::string TEST_FILE = "data/bench_full_system.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(100000000);  // 100M msg/s — effectively no throttle

  BenchTimer timer;
  timer.start();

  recorder.start();
  client.start();
  server.start();

  server.waitForComplete();

  // Wait for client and recorder to finish processing
  int wait_count = 0;
  while ((client.getProcessedCount() < MSG_COUNT ||
          recorder.getRecordedCount() < MSG_COUNT) &&
         wait_count < 200) {
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    wait_count++;
  }

  client.stop();
  recorder.stop();

  double elapsed = timer.elapsed_s();
  double throughput = static_cast<double>(MSG_COUNT) / elapsed;

  std::cout << "\n=== Benchmark: Full System Throughput ===" << std::endl;
  std::cout << "  Messages:    " << MSG_COUNT << std::endl;
  std::cout << "  Elapsed:     " << std::fixed << std::setprecision(3)
            << elapsed * 1000.0 << " ms" << std::endl;
  std::cout << "  Throughput:  " << std::fixed << std::setprecision(2)
            << throughput / 1e6 << " M msg/s" << std::endl;
  std::cout << "  Client processed:  " << client.getProcessedCount()
            << std::endl;
  std::cout << "  Recorder written:  " << recorder.getRecordedCount()
            << std::endl;

  // Correctness check
  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);
  ASSERT_EQ(recorder.getRecordedCount(), MSG_COUNT);

  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // No gaps or overwrites
  ASSERT_EQ(client.getMetrics().seq_gap_count.load(), 0);
  ASSERT_EQ(recorder.getMetrics().seq_gap_count.load(), 0);

  // Throughput target: at least 100K msg/s in full system mode
  ASSERT_GT(throughput, 1e5);
}

// ===========================================================================
// Benchmark 12: Recovery latency
//
// Measures the wall-clock time from fault injection to full recovery
// completion.
// ===========================================================================
TEST(Benchmark, RecoveryLatency) {
  const int64_t MSG_COUNT = 100000;
  const std::string TEST_FILE = "data/bench_recovery.bin";

  auto buffer = std::make_unique<RingBuffer<DEFAULT_RING_BUFFER_SIZE>>();
  MktDataServer server(*buffer);
  MktDataClient client(*buffer, TEST_FILE);
  MktDataRecorder recorder(*buffer, TEST_FILE);

  server.setMessageCount(MSG_COUNT);
  server.setMessageRate(100000);

  recorder.start();
  client.start();
  server.start();

  // Wait until 50% done
  while (client.getLastSeq() < MSG_COUNT / 2 && server.isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(1));
  }

  // Measure recovery time
  BenchTimer recovery_timer;
  recovery_timer.start();

  client.triggerFault(FaultType::CLIENT_CRASH);
  client.waitForRecovery();

  double recovery_ms = recovery_timer.elapsed_ms();

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

  std::cout << "\n=== Benchmark: Recovery Latency ===" << std::endl;
  std::cout << "  Fault at seq: ~" << MSG_COUNT / 2 << std::endl;
  std::cout << "  Recovery time: " << std::fixed << std::setprecision(2)
            << recovery_ms << " ms" << std::endl;
  std::cout << "  Client processed: " << client.getProcessedCount()
            << std::endl;

  ASSERT_EQ(client.getProcessedCount(), MSG_COUNT);

  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  ASSERT_LT(diff, 1e-6);

  // Recovery should complete within 5 seconds
  ASSERT_LT(recovery_ms, 5000.0);
}

// ===========================================================================
// Main — standalone runner (non-GTest fallback)
// ===========================================================================
#ifndef GTEST_FOUND

int main() {
  std::cout << "============================================" << std::endl;
  std::cout << "  ReplaySystem Performance Benchmark Suite" << std::endl;
  std::cout << "============================================" << std::endl;

  // Component-level benchmarks
  RUN_TEST(Benchmark, RingBufferPushLatency);
  RUN_TEST(Benchmark, RingBufferReadLatency);
  RUN_TEST(Benchmark, RingBufferSPSCThroughput);
  RUN_TEST(Benchmark, RingBufferSPMCThroughput);
  RUN_TEST(Benchmark, RingBufferBatchPush);

  // I/O benchmarks
  RUN_TEST(Benchmark, FileWriteThroughput);
  RUN_TEST(Benchmark, FileReadThroughput);
  RUN_TEST(Benchmark, ReplayEngineThroughput);

  // Concurrency benchmarks
  RUN_TEST(Benchmark, SpinLockContention);
  RUN_TEST(Benchmark, EndToEndLatency);

  // System-level benchmarks
  RUN_TEST(Benchmark, FullSystemThroughput);
  RUN_TEST(Benchmark, RecoveryLatency);

  std::cout << "\n============================================" << std::endl;
  std::cout << "  All benchmarks completed!" << std::endl;
  std::cout << "============================================" << std::endl;
  return 0;
}

#endif
