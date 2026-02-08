#pragma once

#include <atomic>
#include <thread>

namespace replay {

// Spin lock implementation
class SpinLock {
 public:
  SpinLock() : flag_(false) {}

  // Disable copy and move
  SpinLock(const SpinLock&) = delete;
  SpinLock& operator=(const SpinLock&) = delete;
  SpinLock(SpinLock&&) = delete;
  SpinLock& operator=(SpinLock&&) = delete;

  void lock() {
    // Use test-and-test-and-set strategy to reduce cache line contention
    while (true) {
      // First check lock state (read-only, won't cause cache line invalidation)
      if (!flag_.load(std::memory_order_relaxed)) {
        // Try to acquire lock
        if (!flag_.exchange(true, std::memory_order_acquire)) {
          return;  // Successfully acquired lock
        }
      }
      // Briefly yield CPU
      std::this_thread::yield();
    }
  }

  bool try_lock() {
    // First check lock state
    if (flag_.load(std::memory_order_relaxed)) {
      return false;
    }
    // Try to acquire lock
    return !flag_.exchange(true, std::memory_order_acquire);
  }

  void unlock() { flag_.store(false, std::memory_order_release); }

 private:
  std::atomic<bool> flag_;
};

// RAII lock guard
class SpinLockGuard {
 public:
  explicit SpinLockGuard(SpinLock& lock) : lock_(lock) { lock_.lock(); }

  ~SpinLockGuard() { lock_.unlock(); }

  // Disable copy and move
  SpinLockGuard(const SpinLockGuard&) = delete;
  SpinLockGuard& operator=(const SpinLockGuard&) = delete;
  SpinLockGuard(SpinLockGuard&&) = delete;
  SpinLockGuard& operator=(SpinLockGuard&&) = delete;

 private:
  SpinLock& lock_;
};

}  // namespace replay
