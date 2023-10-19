#pragma once

#include "magic_enum.hpp"

namespace hyrise {

#ifdef __APPLE__
// OS pages on Mac OS are 16 KiB according to https://developer.apple.com/library/archive/documentation/Performance/Conceptual/ManagingMemory/Articles/AboutMemory.html
constexpr size_t OS_PAGE_SIZE = 16384;
enum class PageSizeType { KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#elif __linux__
// OS pages on Linux are usually 4 KiB
constexpr size_t OS_PAGE_SIZE = 4096;
enum class PageSizeType { KiB4, KiB8, KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#endif

// Get the number of bytes for a given PageSizeType
constexpr inline size_t bytes_for_size_type(const PageSizeType size) {
  return OS_PAGE_SIZE << static_cast<size_t>(size);
}

// Get number of PageSizeTypes
constexpr size_t PAGE_SIZE_TYPES_COUNT = magic_enum::enum_count<PageSizeType>();

// Get the minimum PageSizeType. KiB16 on Mac OS and KiB4 on Linux
constexpr PageSizeType MIN_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(0);

// Get the maximum PageSizeType. MiB2 on Mac OS and Linux
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(PAGE_SIZE_TYPES_COUNT - 1);

// Get the number of bits required to store a PageSizeType
constexpr size_t PAGE_SIZE_TYPE_BITS = std::bit_width(PAGE_SIZE_TYPES_COUNT);

/**
 * PageIDs are used for addressing pages. They consist of a valid flag, a PageSizeType and an index. A Page ID can be unambiguously
 * converted into a virtual address and back. If the valid flag is set, the page is stored in the buffer pool. Otherwise, the page
 * does not exist or the virtual memory address is outside of the buffer pool.
 * 
 * For the implementation, we use C++ bitfields to compress multiple fields into a single 64-bit values without manual shifting. 
 * The valid flag is stored in the least significant bit. The PageSizeType is stored in the next PAGE_SIZE_TYPE_BITS bits. The index 
 * is stored in the remaining bits.
*/
struct PageID {
  using PageIDType = uint64_t;

  PageID() = default;

  constexpr PageID(const PageSizeType size_type, const PageIDType index, bool valid = true)
      : _valid(valid), _size_type(static_cast<PageIDType>(size_type)), _index(index) {}

  // Get the PageSizeType for the page
  PageSizeType size_type() const {
    return magic_enum::enum_value<PageSizeType>(_size_type);
  }

  // Get the number of bytes for the page
  size_t byte_count() const {
    return bytes_for_size_type(size_type());
  }

  // Get the index of the page of a size type
  PageIDType index() const {
    return _index;
  }

  // Returns if the page id is valid or not in the buffer pool
  bool valid() const {
    return _valid;
  }

  bool operator==(const PageID& other) const {
    return (_valid == other._valid == 0) || (_size_type == other._size_type && _index == other._index);
  }

 private:
  PageIDType _valid : 1;
  PageIDType _size_type : PAGE_SIZE_TYPE_BITS;
  PageIDType _index : sizeof(PageIDType) * std::numeric_limits<unsigned char>::digits - PAGE_SIZE_TYPE_BITS - 1;
};

inline std::ostream& operator<<(std::ostream& os, const PageID& page_id) {
  auto ss = std::stringstream{};
  ss << "PageID(valid = " << page_id.valid() << ", size_type = " << magic_enum::enum_name(page_id.size_type())
     << ", index = " << page_id.index() << ")";
  os << ss.str();
  return os;
}

// The invalid page id is used to indicate that a page id is not part of a buffer pool. The valid flag is set to false. All other values are ignored.
static constexpr PageID INVALID_PAGE_ID = PageID{MIN_PAGE_SIZE_TYPE, 0, false};

}  // namespace hyrise
