#pragma once

#include <sched.h>
#include <unistd.h>

#include <cerrno>
#include <cstring>
#include <string>

#include "common/Logging.hpp"

namespace replay {

// Default value meaning "don't pin to any specific core"
constexpr int CPU_CORE_UNSET = -1;

/// Set the CPU affinity of the **calling** thread to the given core.
/// Uses Linux sched_setaffinity(2) with tid=0 (current thread).
///
/// @param core_id  Logical CPU core ID (0-based). If CPU_CORE_UNSET (-1),
///                 the call is a no-op and returns true.
/// @param name     Optional descriptive name used in log messages.
/// @return true on success or no-op, false on failure.
inline bool setCpuAffinity(int core_id,
                           const std::string& name = "thread") {
  if (core_id == CPU_CORE_UNSET) {
    return true;  // no-op
  }

  int num_cpus = static_cast<int>(sysconf(_SC_NPROCESSORS_ONLN));
  if (core_id < 0 || core_id >= num_cpus) {
    LOG_ERROR(replay::logger(),
              "setCpuAffinity failed for {}: core_id={} out of range [0, {})",
              name, core_id, num_cpus);
    return false;
  }

  cpu_set_t cpuset;
  CPU_ZERO(&cpuset);
  CPU_SET(core_id, &cpuset);

  // sched_setaffinity with pid=0 targets the calling thread (on Linux,
  // each thread has its own tid and pid=0 is an alias for the caller).
  int rc = sched_setaffinity(0, sizeof(cpu_set_t), &cpuset);
  if (rc != 0) {
    LOG_ERROR(replay::logger(),
              "sched_setaffinity failed for {} on core {}: {}",
              name, core_id, strerror(errno));
    return false;
  }

  LOG_INFO(replay::logger(),
           "CPU affinity set: {}  ->  core {}", name, core_id);
  return true;
}

}  // namespace replay
