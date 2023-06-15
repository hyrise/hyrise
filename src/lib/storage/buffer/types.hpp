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

STRONG_TYPEDEF(int8_t, NumaMemoryNode);

namespace hyrise {

enum class PageSizeType { KiB8, KiB16, KiB32, KiB64, KiB128, KiB256, KiB512 };

// Get the number of bytes for a given PageSizeType
constexpr size_t bytes_for_size_type(const PageSizeType size) {
  return 1 << (13 + static_cast<size_t>(size));
}

// Find the smallest PageSizeType that can hold the given bytes
constexpr PageSizeType find_fitting_page_size_type(const std::size_t bytes) {
  for (auto page_size_type : magic_enum::enum_values<PageSizeType>()) {
    if (bytes <= bytes_for_size_type(page_size_type)) {
      return page_size_type;
    }
  }
  Fail("Cannot fit value of " + std::to_string(bytes) + " bytes to a PageSizeType");
}

constexpr size_t NUM_PAGE_SIZE_TYPES = magic_enum::enum_count<PageSizeType>();

constexpr PageSizeType MIN_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(0);
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(NUM_PAGE_SIZE_TYPES - 1);
constexpr size_t PAGE_SIZE_TYPE_BITS = boost::static_log2<NUM_PAGE_SIZE_TYPES>::value + 1;

struct PageID {
  using PageIDType = uint64_t;
  PageIDType _size_type : PAGE_SIZE_TYPE_BITS;
  PageIDType index : (sizeof(PageIDType) * CHAR_BIT - PAGE_SIZE_TYPE_BITS);

  PageSizeType size_type() const {
    return magic_enum::enum_value<PageSizeType>(this->_size_type);
  }

  bool valid() const {
    Fail("TODO");
  }

  auto operator<=>(const PageID&) const = default;

  PageID() = default;

  PageID(const PageSizeType size_type, const PageIDType index)
      : _size_type(static_cast<PageIDType>(size_type)), index(index) {}
};

// Signifies an invalid NUMA node (>= 0 is a valid node)
constexpr auto NO_NUMA_MEMORY_NODE = NumaMemoryNode{-1};

// The usual numa node for DRAM allocations
constexpr auto DEFAULT_DRAM_NUMA_NODE = NumaMemoryNode{0};

// Pages need to be aligned to 512 in order to be used with O_DIRECT
constexpr size_t PAGE_ALIGNMENT = 512;

// Default page size in Linux, TODO: check with OS X
constexpr size_t PAGE_SIZE = 4096;

static_assert(bytes_for_size_type(magic_enum::enum_value<PageSizeType>(0)) >= PAGE_SIZE,
              "Smallest page size does not fit into an OS page");

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
using StateVersionType = uint64_t;

// Item for the Eviction Queue
struct EvictionItem {
  // The page to be evicted.
  PageID page_id;

  // Insertion timestamp for frame into the queue. Is compared with eviction_timestamp of frame.
  uint64_t timestamp;

  // Check if the given frame can be evicted if it was marked before
  bool can_evict(StateVersionType state_and_version) const;

  // Check if the given frame can be marked for eviction
  bool can_mark(StateVersionType state_and_version) const;
};

constexpr size_t MAX_EVICTION_QUEUE_PURGES = 1024;

constexpr size_t DEFAULT_RESERVED_VIRTUAL_MEMORY = 1UL << 32;
constexpr size_t DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION = DEFAULT_RESERVED_VIRTUAL_MEMORY / NUM_PAGE_SIZE_TYPES;

constexpr size_t INITIAL_SLOTS_PER_REGION = 100000;  // TODO

using EvictionQueue = tbb::concurrent_queue<EvictionItem>;

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

boost::container::pmr::memory_resource* get_buffer_manager_memory_resource();

}  // namespace hyrise