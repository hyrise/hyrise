#pragma once

#include <tbb/concurrent_queue.h>
#include <bit>
#include <boost/describe.hpp>
#include <boost/describe/enum.hpp>
#include <boost/mp11.hpp>
#include <boost/mp11/list.hpp>
#include <limits>
#include "boost/integer/static_log2.hpp"
#include "storage/buffer/types.hpp"
#include "strong_typedef.hpp"
#include "utils/assert.hpp"

// Declare types here to avoid problems with circular dependency
STRONG_TYPEDEF(uint32_t, PageID);
STRONG_TYPEDEF(uint32_t, FrameID);

namespace hyrise {
constexpr PageID INVALID_PAGE_ID{std::numeric_limits<PageID::base_type>::max()};
constexpr FrameID INVALID_FRAME_ID{std::numeric_limits<FrameID::base_type>::max()};

// TODO: Check magic_enum
BOOST_DEFINE_ENUM_CLASS(PageSizeType, KiB8, KiB16, KiB32, KiB64, KiB128);

constexpr size_t bytes_for_size_type(const PageSizeType size) {
  return 1 << (13 + static_cast<size_t>(size));
}

constexpr PageSizeType find_fitting_page_size_type(const std::size_t value) {
  if (value <= bytes_for_size_type(PageSizeType::KiB8)) {
    return PageSizeType::KiB8;
  } else if (value <= bytes_for_size_type(PageSizeType::KiB16)) {
    return PageSizeType::KiB16;
  } else if (value <= bytes_for_size_type(PageSizeType::KiB32)) {
    return PageSizeType::KiB32;
  } else if (value <= bytes_for_size_type(PageSizeType::KiB64)) {
    return PageSizeType::KiB64;
  } else if (value <= bytes_for_size_type(PageSizeType::KiB128)) {
    return PageSizeType::KiB128;
  }
  Fail("Cannot fit value to a PageSizeType");
}

constexpr size_t NUM_PAGE_SIZE_TYPES = boost::mp11::mp_size<boost::describe::describe_enumerators<PageSizeType>>::value;
constexpr PageSizeType MIN_PAGE_SIZE_TYPE = PageSizeType::KiB8;
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = PageSizeType::KiB128;
constexpr size_t MAX_PAGE_OFFSET = boost::static_log2<bytes_for_size_type(MAX_PAGE_SIZE_TYPE)>::value;
constexpr size_t BITS_PAGE_SIZE_TYPES = std::bit_width(NUM_PAGE_SIZE_TYPES);

constexpr auto NO_NUMA_MEMORY_NODE = -1;

// Pages need to be aligned to 512 in order to be used with O_DIRECT
constexpr size_t PAGE_ALIGNMENT = 512;

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

// Eviction Queue
struct EvictionItem {
  Frame* frame;
  uint64_t timestamp;
};

using EvictionQueue = tbb::concurrent_queue<EvictionItem>;

enum BufferManagerMode {
  DramDramSSD,       // Use 2 Volatile Regions and SSD
  DramSSD,           // Use 1 Volatile Region and SSD
  DramNumaRgionSSD,  // Use 1 Volatile Region, 1 NUMA Region and SSD
  NumaRegionSSD      // Use 1 NUMA Region and SSD
};

}  // namespace hyrise