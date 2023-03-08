#pragma once

#include <bit>
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

enum class PageSizeType {
  KiB8,    // = 1 << 13,
  KiB16,   // = 1 << 14,
  KiB32,   // = 1 << 15,
  KiB64,   // = 1 << 16,
  KiB128,  // = 1 << 17,
};

constexpr size_t bytes_for_size_type(const PageSizeType size) {
  return 1 << (13 + static_cast<size_t>(size));
}

constexpr size_t NUM_PAGE_SIZE_TYPES = 5;  // Total count of page size types
constexpr PageSizeType MIN_PAGE_SIZE_TYPE = PageSizeType::KiB128;
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = PageSizeType::KiB128;
constexpr size_t MAX_PAGE_OFFSET = boost::static_log2<bytes_for_size_type(MAX_PAGE_SIZE_TYPE)>::value;
constexpr size_t BITS_PAGE_SIZE_TYPES = std::bit_width(NUM_PAGE_SIZE_TYPES);

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

}  // namespace hyrise