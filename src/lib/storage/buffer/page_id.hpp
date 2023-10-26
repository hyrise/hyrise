#pragma once

#include <bit>

#include "magic_enum.hpp"

namespace hyrise {

#ifdef __APPLE__
// OS pages on Mac OS are 16 KiB according to
// https://developer.apple.com/library/archive/documentation/Performance/Conceptual/ManagingMemory/Articles/AboutMemory.html.
constexpr uint64_t OS_PAGE_SIZE = 16384;
#elif __linux__
// OS pages on Linux are usually 4 KiB.
constexpr uint64_t OS_PAGE_SIZE = 4096;
#endif

// Page sizes are always a multiple of the OS page size and increase by powers of two.
// The smallest page size is 16 KiB on Mac OS and 4 KiB on Linux.
#ifdef __APPLE__
enum class PageSizeType { KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#elif __linux__
enum class PageSizeType { KiB4, KiB8, KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#endif

// Get the number of bytes for a given PageSizeType.
constexpr inline uint64_t bytes_for_size_type(const PageSizeType size) {
  // We assume that the OS page size is either 4 KiB on Linux or 16 KiB on Mac OS.
  // The page size types increase by power of two.
  return OS_PAGE_SIZE << static_cast<uint64_t>(size);
}

// The number of PageSizeTypes.
constexpr uint64_t PAGE_SIZE_TYPES_COUNT = magic_enum::enum_count<PageSizeType>();

// Get the minimum PageSizeType. KiB16 on Mac OS and KiB4 on Linux
constexpr PageSizeType MIN_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(0);

// The maximum PageSizeType. MiB2 on Mac OS and Linux.
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(PAGE_SIZE_TYPES_COUNT - 1);

// Get the number of bits required to store a PageSizeType
// TODO(nikriek): Replace with std::bit_width once we get rid of gcc9
constexpr uint64_t PAGE_SIZE_TYPE_BITS =
    std::numeric_limits<uint64_t>::digits - std::countl_zero(PAGE_SIZE_TYPES_COUNT);

/**
 * PageIDs are used for addressing pages. They consist of a validity flag, a PageSizeType, and an index. A PageID can be unambiguously
 * converted into a virtual address and back. The valid flag indicates that the page is stored in the buffer pool. Otherwise, the page
 * does not exist or the virtual memory address is outside of the buffer pool.
 * 
 * For the implementation, we use C++ bitfields to compress multiple fields into a single 64-bit values without manual bit shifting. 
 * The valid flag is stored in the most significant bit. The PageSizeType is stored in the next PAGE_SIZE_TYPE_BITS bits. The index 
 * is stored in the remaining bits.
*/
struct PageID {
  using PageIDType = uint64_t;

  constexpr explicit PageID(const PageSizeType size_type, const uint64_t index, bool valid = true)
      : _valid(valid), _size_type(static_cast<PageIDType>(size_type)), _index(index) {}

  // Get the PageSizeType for the page
  PageSizeType size_type() const {
    return magic_enum::enum_value<PageSizeType>(_size_type);
  }

  // Get the number of bytes for the page.
  uint64_t byte_count() const {
    return bytes_for_size_type(size_type());
  }

  // Get the index of the page of a size type.
  uint64_t index() const {
    return _index;
  }

  // Returns if the PageID is valid or not in the buffer pool.
  bool valid() const {
    return _valid;
  }

  bool operator==(const PageID& other) const {
    return (_valid == other._valid) &&
           (!_valid || (_size_type == other._size_type && _index == other._index));
  }

  bool operator!=(const PageID& other) const {
    return !operator==(other);
  }

 private:
  PageIDType _valid : 1;
  PageIDType _size_type : PAGE_SIZE_TYPE_BITS;
  PageIDType _index : sizeof(PageIDType) * std::numeric_limits<unsigned char>::digits - PAGE_SIZE_TYPE_BITS - 1;
};

static_assert(sizeof(PageID) == 8, "PageID must be 64 bit");

inline std::ostream& operator<<(std::ostream& os, const PageID& page_id) {
  os << "PageID(valid = " << page_id.valid() << ", size_type = " << magic_enum::enum_name(page_id.size_type())
     << ", index = " << page_id.index() << ")";
  return os;
}

// The invalid PageID is used to indicate that a PageID is not part of a buffer pool. The valid flag is set to false.
// All other values are ignored.
static constexpr PageID INVALID_PAGE_ID = PageID{MIN_PAGE_SIZE_TYPE, 0, false};

}  // namespace hyrise
