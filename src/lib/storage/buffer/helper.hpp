#pragma once

#include <bit>
#include <iostream>
#include <limits>
#include "config.hpp"
#include "frame.hpp"
#include "metrics.hpp"
#include "page_id.hpp"
#include "strong_typedef.hpp"
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

namespace hyrise {

constexpr size_t DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION =
    (BufferManagerConfig::DEFAULT_RESERVED_VIRTUAL_MEMORY / NUM_PAGE_SIZE_TYPES) /
    bytes_for_size_type(MAX_PAGE_SIZE_TYPE) * bytes_for_size_type(MAX_PAGE_SIZE_TYPE);

// Hints the buffer manager about the access intent of the caller. This influences the migration strategy
enum class AccessIntent { Read, Write };

inline void DebugAssertPageAligned(const void* data) {
  constexpr size_t PAGE_ALIGNMENT = 512;
  DebugAssert(reinterpret_cast<std::uintptr_t>(data) % PAGE_ALIGNMENT == 0,
              "Destination is not properly aligned to 512: " +
                  std::to_string(reinterpret_cast<std::uintptr_t>(data) % PAGE_ALIGNMENT));
}

template <typename Func>
inline void retry_with_backoff(const Func& func, const size_t max_repeat_count = 1000000) {
  // TODO: This function could be improved with a "Parking Lot" as described in
  // Böttcher et al. "Scalable and Robust Latches for Database Systems"
  for (auto repeat = size_t{0}; repeat < max_repeat_count; ++repeat) {
    if (func()) {
      return;
    }
    if (repeat < 4) {
    } else if ((repeat < 32) || (repeat & 1)) {
      std::this_thread::yield();
    } else if (repeat < 1000000) {
      std::this_thread::sleep_for(std::chrono::nanoseconds(1000));
    } else {
      Fail("Yield for too long. Something is blocking. Current state");
    }
  }
  Fail("Too many retries. Something is blocking");
}

}  // namespace hyrise
