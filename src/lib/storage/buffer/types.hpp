#pragma once

#include <tbb/concurrent_queue.h>
#include <bit>
#include <limits>
#include <magic_enum.hpp>
#include "boost/integer/static_log2.hpp"
#include "strong_typedef.hpp"
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

// Declare types here to avoid problems with circular dependency

STRONG_TYPEDEF(uint32_t, PageID);

namespace hyrise {
constexpr PageID INVALID_PAGE_ID{std::numeric_limits<PageID::base_type>::max()};

enum class PageSizeType { KiB8, KiB16, KiB32, KiB64, KiB128, KiB256 };  //, KiB512 };

constexpr size_t bytes_for_size_type(const PageSizeType size) {
  return 1 << (13 + static_cast<size_t>(size));
}

constexpr PageSizeType find_fitting_page_size_type(const std::size_t value) {
  for (auto page_size_type : magic_enum::enum_values<PageSizeType>()) {
    if (value <= bytes_for_size_type(page_size_type)) {
      return page_size_type;
    }
  }
  Fail("Cannot fit value of " + std::to_string(value) + " bytes to a PageSizeType");
}

constexpr size_t NUM_PAGE_SIZE_TYPES = magic_enum::enum_count<PageSizeType>();
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(NUM_PAGE_SIZE_TYPES - 1);

constexpr auto NO_NUMA_MEMORY_NODE = -1;

// Pages need to be aligned to 512 in order to be used with O_DIRECT
constexpr size_t PAGE_ALIGNMENT = 512;

// How often old items should be evicted from the eviction queue
constexpr static std::chrono::milliseconds IDLE_EVICTION_QUEUE_PURGE = std::chrono::milliseconds(1000);

// Copied from boost::interprocess, because #include <boost/type_traits/add_reference.hpp> was not enough
// I guess, because of "typedef nat &type" that can be used as reference dummy type
struct nat {};

template <typename T>
struct add_reference {
  typedef T& type;
};

template <class T>
struct add_reference<T&> {
  typedef T& type;
};

template <>
struct add_reference<void> {
  typedef nat& type;
};

template <>
struct add_reference<const void> {
  typedef const nat& type;
};

class Frame;

// Item for the Eviction Queue
struct EvictionItem {
  std::weak_ptr<Frame> frame;

  // Insert timestamp for frame into the queue. Is compared with eviction_timestamp of frame.
  uint64_t timestamp;

  bool can_evict(std::shared_ptr<Frame>& frame) const;
};

// using PageTable = boost::unordered_flat_map<PageID, std::shared_ptr<SharedFrame>>;

using EvictionQueue = tbb::concurrent_queue<EvictionItem>;

// The page type is used to determine the memory region the page is allocated on
enum class PageType { Invalid, Dram, Numa };

// Hints the buffer manager about the access intent of the caller. AccessIntent.Write is usually used during allocations for example.
enum class AccessIntent { Read, Write };

enum BufferManagerMode {
  // Use two volatile regions (DRAM and DRAM for NUMA emulation purposes) and SSD
  DramNumaEmulationSSD,

  // Use one volatile region (DRAM) and SSD
  DramSSD,

  // Use two volatile regions, one with DRAM and one NUMA, and SSD
  DramNumaSSD,

  // Use one volatile region (NUMA) and SSD
  NumaSSD
};

constexpr PageType page_type_for_dram_buffer_pools(const BufferManagerMode mode) {
  switch (mode) {
    case BufferManagerMode::DramNumaEmulationSSD:
    case BufferManagerMode::DramSSD:
    case BufferManagerMode::DramNumaSSD:
      return PageType::Dram;
    case BufferManagerMode::NumaSSD:
      return PageType::Invalid;
  }
  Fail("Unknown BufferManagerMode");
}

constexpr PageType page_type_for_numa_buffer_pools(const BufferManagerMode mode) {
  switch (mode) {
    case BufferManagerMode::DramNumaEmulationSSD:
      return PageType::Dram;
    case BufferManagerMode::DramNumaSSD:
      return PageType::Numa;
    case BufferManagerMode::DramSSD:
      return PageType::Invalid;
    case BufferManagerMode::NumaSSD:
      return PageType::Numa;
  }
  Fail("Unknown BufferManagerMode");
}

}  // namespace hyrise