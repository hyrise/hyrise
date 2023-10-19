#pragma once
#include <magic_enum.hpp>
#include "boost/integer/static_log2.hpp"

namespace hyrise {

#ifdef __APPLE__
constexpr size_t OS_PAGE_SIZE = 16384;
enum class PageSizeType { KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#elif __linux__
constexpr size_t OS_PAGE_SIZE = 4096;
enum class PageSizeType { KiB4, KiB8, KiB16, KiB32, KiB64, KiB128, KiB256, KiB512, MiB1, MiB2 };
#endif

// Get the number of bytes for a given PageSizeType
constexpr inline size_t bytes_for_size_type(const PageSizeType size) {
  return OS_PAGE_SIZE << static_cast<size_t>(size);
}

constexpr size_t NUM_PAGE_SIZE_TYPES = magic_enum::enum_count<PageSizeType>();
constexpr PageSizeType MIN_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(0);
constexpr PageSizeType MAX_PAGE_SIZE_TYPE = magic_enum::enum_value<PageSizeType>(NUM_PAGE_SIZE_TYPES - 1);
constexpr size_t PAGE_SIZE_TYPE_BITS = boost::static_log2<NUM_PAGE_SIZE_TYPES>::value + 1;

/**
 * PageIDs are used for addressing pages. They consist of a valid flag, a PageSizeType and an index. A Page ID can be unambiguously
 * converted into a virtual address and back. If the valid flag is set, the page is stored in the buffer pool. Otherwise, the page
 * does not exist or the virtual memory address is outside of the buffer pool.
*/
struct PageID {
  using PageIDType = uint64_t;

  PageIDType _valid : 1;
  PageIDType _size_type : PAGE_SIZE_TYPE_BITS;
  PageIDType index : (sizeof(PageIDType) * CHAR_BIT - PAGE_SIZE_TYPE_BITS - 1);

  PageSizeType size_type() const {
    return magic_enum::enum_value<PageSizeType>(this->_size_type);
  }

  size_t num_bytes() const {
    return bytes_for_size_type(size_type());
  }

  bool valid() const {
    return _valid;
  }

  bool operator<(const PageID& other) const {
    return this->index < other.index && this->size_type() < other.size_type();
  };

  bool operator==(const PageID& other) const {
    return this->index == other.index && this->size_type() == other.size_type();
  };

  PageID() = default;

  constexpr PageID(const PageSizeType size_type, const uint64_t index, bool valid = true)
      : _valid(valid), _size_type(static_cast<PageIDType>(size_type)), index(index) {}
};

inline std::ostream& operator<<(std::ostream& os, const PageID& page_id) {
  os << "PageID(valid=" << page_id.valid() << ", size_type=" << magic_enum::enum_name(page_id.size_type())
     << ", index=" << page_id.index << ")";
  return os;
}

static constexpr PageID INVALID_PAGE_ID = PageID{MIN_PAGE_SIZE_TYPE, 0, false};
}  // namespace hyrise