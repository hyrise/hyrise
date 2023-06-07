#pragma once

#include <atomic>
#include <mutex>
#include <shared_mutex>
#include "utils/assert.hpp"

/**
 * A Hybrid Latch combines the best of pessimistic and optimistic locking. It based on a RW-Mutex for exclusive and shared locking. 
 * For optimistic the latch uses a version number that is incremented on every exclusive lock.
 * 
 * The code is mainly taken from "Scalable and Robust Latches for Database Systems" (https://dl.acm.org/doi/10.1145/3399666.3399908)
*/
class HybridLatch {
  using VersionType = uint64_t;

  std::shared_mutex _rw_lock;
  std::atomic<VersionType> _version;

  constexpr static uint64_t EXCLUSIVE_FLAG_MASK = 1ULL << 63;

 public:
  void lock_shared() {
    _rw_lock.lock_shared();
  }

  void unlock_shared() {
    _rw_lock.unlock_shared();
  }

  void lock_exclusive() {
    _rw_lock.lock();
    _version |= EXCLUSIVE_FLAG_MASK;
  }

  std::lock_guard<std::shared_mutex> create_exclusive_guard() {
    lock_exclusive();
    return std::lock_guard(_rw_lock, std::adopt_lock);
  }

  void unlock_exclusive() {
    _version.fetch_add(1);
    _version &= ~EXCLUSIVE_FLAG_MASK;
    _rw_lock.unlock();
  }

  bool is_locked_exclusive() {
    return (_version.load() & EXCLUSIVE_FLAG_MASK) == EXCLUSIVE_FLAG_MASK;
  }

  template <typename Lambda>
  bool try_read_optimistically(const Lambda& read_callback) {
    if (is_locked_exclusive()) {
      return false;
    }
    auto pre_version = _version.load();
    read_callback();
    if (is_locked_exclusive()) {
      return false;
    }

    return pre_version == _version.load();
  }

  template <typename ReadLambda, typename RestartLambda>
  void read_optimistic_if_possible(const ReadLambda& read_callback, const RestartLambda& restart_callback) {
    if (!try_read_optimistically(read_callback)) {
      restart_callback();
      lock_shared();
      read_callback();
      unlock_shared();
    }
  }
};