#pragma once

#include <atomic>
#include <cstdint>

namespace hyrise {

/**
 * Thread-safe allocation of ids unique in some context (e.g. TaskIDs, WorkerIDs...) starting from 0 and incrementing
 */
class UidAllocator {
 public:
  std::uint32_t allocate() {
    return _incrementor++;
  }

 private:
  std::atomic_uint32_t _incrementor{0};
};
}  // namespace hyrise
