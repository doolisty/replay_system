#include <chrono>
#include <cstring>
#include <ctime>
#include <iomanip>
#include <iostream>
#include <memory>
#include <sstream>
#include <string>
#include <string_view>
#include <vector>

#include "client/MktDataClient.hpp"
#include "common/CpuAffinity.hpp"
#include "common/Logging.hpp"
#include "common/RingBuffer.hpp"
#include "recorder/MktDataRecorder.hpp"
#include "server/MktDataServer.hpp"

namespace {

// Get current date string YYYYMMDD
std::string getDateString() {
  auto now = std::chrono::system_clock::now();
  auto time = std::chrono::system_clock::to_time_t(now);
  std::tm tm = *std::localtime(&time);

  std::ostringstream oss;
  oss << std::put_time(&tm, "%Y%m%d");
  return oss.str();
}

// Print usage instructions
void printUsage(std::string_view program) {
  std::cout
      << "Usage: " << program << " [options]\n"
      << "\nOptions:\n"
      << "  --mode=<mode>        Run mode: test, recovery_test, stress\n"
      << "  --messages=<count>   Message count (default: 10000)\n"
      << "  --rate=<rate>        Messages per second (default: 1000)\n"
      << "  --fault-at=<seq>     Trigger fault at specified sequence number "
         "(recovery_test mode)\n"
      << "  --data-dir=<dir>     Data directory, output files written to this "
         "directory (default: data)\n"
      << "  --output=<file>      Output file path (overrides --data-dir)\n"
      << "  --cpu=<c0,c1,...>    Pin threads to CPU cores (comma-separated)\n"
      << "                       Order: main, server, client, recorder\n"
      << "                       Unspecified threads are not pinned\n"
      << "  --help               Show help information\n"
      << std::endl;
}

// Parse comma-separated CPU core IDs, e.g. "0,1,2,3"
std::vector<int> parseCpuCores(std::string_view str) {
  std::vector<int> cores;
  std::string token;
  std::istringstream ss{std::string(str)};
  while (std::getline(ss, token, ',')) {
    if (!token.empty()) {
      cores.push_back(std::stoi(token));
    }
  }
  return cores;
}

// Thread index for CPU core assignment order
enum CpuSlot : size_t {
  SLOT_MAIN = 0,
  SLOT_SERVER = 1,
  SLOT_CLIENT = 2,
  SLOT_RECORDER = 3,
  SLOT_COUNT
};

// Parse command line arguments
struct Config {
  std::string mode = "test";
  int64_t message_count = 10000;
  int64_t message_rate = 1000;
  int64_t fault_at = -1;
  std::string output_file;

  // CPU affinity: core IDs for each thread (-1 = unset, no pinning)
  // Order: [main, server, client, recorder]
  int cpu_main = replay::CPU_CORE_UNSET;
  int cpu_server = replay::CPU_CORE_UNSET;
  int cpu_client = replay::CPU_CORE_UNSET;
  int cpu_recorder = replay::CPU_CORE_UNSET;

  Config() { output_file = "data/mktdata_" + getDateString() + ".bin"; }

  // Assign CPU cores from a list (order: main, server, client, recorder).
  // Slots beyond the list size are left as CPU_CORE_UNSET.
  void assignCpuCores(const std::vector<int>& cores) {
    int* slots[] = {&cpu_main, &cpu_server, &cpu_client, &cpu_recorder};
    for (size_t i = 0; i < SLOT_COUNT; ++i) {
      *slots[i] = (i < cores.size()) ? cores[i] : replay::CPU_CORE_UNSET;
    }
  }
};

Config parseArgs(int argc, char* argv[]) {
  Config config;

  for (int i = 1; i < argc; ++i) {
    std::string_view arg = argv[i];

    if (arg == "--help") {
      printUsage(argv[0]);
      std::exit(0);
    } else if (arg.starts_with("--mode=")) {
      config.mode = arg.substr(7);
    } else if (arg.starts_with("--messages=")) {
      config.message_count = std::stoll(std::string(arg.substr(11)));
    } else if (arg.starts_with("--rate=")) {
      config.message_rate = std::stoll(std::string(arg.substr(7)));
    } else if (arg.starts_with("--fault-at=")) {
      config.fault_at = std::stoll(std::string(arg.substr(11)));
    } else if (arg.starts_with("--data-dir=")) {
      std::string dir(arg.substr(11));
      if (!dir.empty() && dir.back() == '/') {
        dir.pop_back();
      }
      config.output_file = dir + "/mktdata_" + getDateString() + ".bin";
    } else if (arg.starts_with("--output=")) {
      config.output_file = std::string(arg.substr(9));
    } else if (arg.starts_with("--cpu=")) {
      config.assignCpuCores(parseCpuCores(arg.substr(6)));
    }
  }

  return config;
}

// Basic functionality test
int runTest(const Config& config) {
  auto* logger = replay::logger();
  std::cout << "=== Basic Functionality Test ===" << std::endl;
  std::cout << "Message count: " << config.message_count << std::endl;
  std::cout << "Send rate: " << config.message_rate << "/s" << std::endl;
  std::cout << "Output file: " << config.output_file << std::endl;
  std::cout << std::endl;

  LOG_INFO(logger, "runTest start: messages={}, rate={}, output={}",
           config.message_count, config.message_rate, config.output_file);

  // Create shared ring buffer (allocated on heap to avoid stack overflow)
  auto buffer =
      std::make_unique<replay::RingBuffer<replay::DEFAULT_RING_BUFFER_SIZE>>();

  // Create components
  replay::MktDataServer server(*buffer);
  replay::MktDataClient client(*buffer, config.output_file);
  replay::MktDataRecorder recorder(*buffer, config.output_file);

  // Configure server
  server.setMessageCount(config.message_count);
  server.setMessageRate(config.message_rate);

  // Set CPU affinity
  server.setCpuCore(config.cpu_server);
  client.setCpuCore(config.cpu_client);
  recorder.setCpuCore(config.cpu_recorder);

  // Start threads
  auto start_time = std::chrono::high_resolution_clock::now();

  recorder.start();
  client.start();
  server.start();

  // Wait for server to complete
  server.waitForComplete();

  // Wait for consumers to finish processing
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Stop components
  client.stop();
  recorder.stop();

  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(
      end_time - start_time);

  // Print results
  std::cout << "\n=== Test Results ===" << std::endl;
  std::cout << "Total time: " << duration.count() << " ms" << std::endl;
  std::cout << "Server sent: " << server.getSentCount() << " messages"
            << std::endl;
  std::cout << "Client processed: " << client.getProcessedCount() << " messages"
            << std::endl;
  std::cout << "Recorder recorded: " << recorder.getRecordedCount()
            << " messages" << std::endl;
  std::cout << "Client sum: " << std::fixed << std::setprecision(6)
            << client.getSum() << std::endl;
  std::cout << "Recorder expected sum: " << std::fixed << std::setprecision(6)
            << recorder.getExpectedSum() << std::endl;

  // Verify results
  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  bool passed = diff < 1e-9;

  std::cout << "\nVerification result: " << (passed ? "PASSED" : "FAILED")
            << std::endl;

  LOG_INFO(logger,
           "runTest complete: sent={}, client_processed={}, "
           "recorder_recorded={}, duration_ms={}, passed={}",
           server.getSentCount(), client.getProcessedCount(),
           recorder.getRecordedCount(), duration.count(), passed);

  return passed ? 0 : 1;
}

// Fault recovery test
int runRecoveryTest(const Config& config) {
  auto* logger = replay::logger();
  std::cout << "=== Fault Recovery Test ===" << std::endl;
  std::cout << "Message count: " << config.message_count << std::endl;
  std::cout << "Fault position: " << config.fault_at << std::endl;
  std::cout << std::endl;

  LOG_INFO(
      logger,
      "runRecoveryTest start: messages={}, rate={}, fault_at={}, output={}",
      config.message_count, config.message_rate, config.fault_at,
      config.output_file);

  // Create shared ring buffer (allocated on heap to avoid stack overflow)
  auto buffer =
      std::make_unique<replay::RingBuffer<replay::DEFAULT_RING_BUFFER_SIZE>>();

  // Create components
  replay::MktDataServer server(*buffer);
  replay::MktDataClient client(*buffer, config.output_file);
  replay::MktDataRecorder recorder(*buffer, config.output_file);

  // Configure server
  server.setMessageCount(config.message_count);
  server.setMessageRate(config.message_rate);

  // Set CPU affinity
  server.setCpuCore(config.cpu_server);
  client.setCpuCore(config.cpu_client);
  recorder.setCpuCore(config.cpu_recorder);

  // Start threads
  recorder.start();
  client.start();
  server.start();

  // Trigger fault at specified position
  while (client.getLastSeq() < config.fault_at && server.isRunning()) {
    std::this_thread::sleep_for(std::chrono::milliseconds(10));
  }

  std::cout << "Triggering fault..." << std::endl;
  client.triggerFault(replay::FaultType::CLIENT_CRASH);

  // Wait for recovery to complete
  client.waitForRecovery();
  std::cout << "Recovery complete" << std::endl;
  LOG_INFO(logger, "Client recovery completed {}", "");

  // Wait for server to complete
  server.waitForComplete();

  // Wait for consumers to finish processing
  std::this_thread::sleep_for(std::chrono::milliseconds(500));

  // Stop components
  client.stop();
  recorder.stop();

  // Print results
  std::cout << "\n=== Test Results ===" << std::endl;
  std::cout << "Client sum: " << std::fixed << std::setprecision(6)
            << client.getSum() << std::endl;
  std::cout << "Recorder expected sum: " << std::fixed << std::setprecision(6)
            << recorder.getExpectedSum() << std::endl;

  // Verify results
  double diff = std::abs(client.getSum() - recorder.getExpectedSum());
  bool passed = diff < 1e-9;

  std::cout << "\nVerification result: " << (passed ? "PASSED" : "FAILED")
            << std::endl;

  LOG_INFO(
      logger,
      "runRecoveryTest complete: client_sum={}, recorder_sum={}, passed={}",
      client.getSum(), recorder.getExpectedSum(), passed);

  return passed ? 0 : 1;
}

// Stress test
int runStressTest(const Config& config) {
  auto* logger = replay::logger();
  std::cout << "=== Stress Test ===" << std::endl;
  std::cout << "Message count: " << config.message_count << std::endl;
  std::cout << "Send rate: " << config.message_rate << "/s" << std::endl;
  std::cout << std::endl;

  LOG_INFO(logger, "runStressTest start: messages={}, rate={}",
           config.message_count, config.message_rate);

  return runTest(config);  // Stress test uses same logic as basic test, only
                           // parameters differ
}

}  // namespace

int main(int argc, char* argv[]) {
  Config config = parseArgs(argc, argv);

  auto* logger = replay::initLogger("replay");
  LOG_INFO(logger,
           "ReplaySystem start: mode={}, messages={}, rate={}, fault_at={}, "
           "output={}",
           config.mode, config.message_count, config.message_rate,
           config.fault_at, config.output_file);

  // Set main thread CPU affinity
  replay::setCpuAffinity(config.cpu_main, "main");

  std::cout << "Real-time Data Replay System" << std::endl;
  std::cout << "=================" << std::endl;
  std::cout << std::endl;

  if (config.mode == "test") {
    return runTest(config);
  } else if (config.mode == "recovery_test") {
    if (config.fault_at < 0) {
      config.fault_at =
          config.message_count / 2;  // Default: trigger fault at half position
    }
    return runRecoveryTest(config);
  } else if (config.mode == "stress") {
    return runStressTest(config);
  } else {
    LOG_ERROR(logger, "Unknown mode: {}", config.mode);
    std::cerr << "Unknown mode: " << config.mode << std::endl;
    printUsage(argv[0]);
    return 1;
  }
}
