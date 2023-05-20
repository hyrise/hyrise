#pragma once

#include <tbb/concurrent_queue.h>
#include <bit>
#include <boost/intrusive_ptr.hpp>
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

// Signifies an invalid NUMA node (>= 0 is a valid node)
constexpr int8_t NO_NUMA_MEMORY_NODE = -1;

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

// A FramePtr embeds the reference couting in the frame itself. This reduces storage space in vectors and it allows us to deal with desired circular dependencies.
using FramePtr = boost::intrusive_ptr<Frame>;

// Item for the Eviction Queue
struct EvictionItem {
  // The frame to be evicted.
  FramePtr frame;

  // Insertion timestamp for frame into the queue. Is compared with eviction_timestamp of frame.
  uint64_t timestamp;

  // Check if the given frame can be evicted based on the timestamp comparison
  bool can_evict() const;
};

constexpr size_t MAX_EVICTION_QUEUE_PURGES = 1024;

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
      return PageType::Numa;
    case BufferManagerMode::DramNumaSSD:
      return PageType::Numa;
    case BufferManagerMode::DramSSD:
      return PageType::Invalid;
    case BufferManagerMode::NumaSSD:
      return PageType::Numa;
  }
  Fail("Unknown BufferManagerMode");
}

// Pointer swizzling
bool is_swizzled_pointer(const std::uintptr_t ptr) noexcept;
std::uintptr_t swizzle_pointer(const std::uintptr_t offset, const std::byte* data) noexcept;
std::uintptr_t unswizzle_pointer(const std::uintptr_t ptr, const std::byte* data) noexcept;

}  // namespace hyrise