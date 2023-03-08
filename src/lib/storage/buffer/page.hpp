#pragma once

#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief Page wraps binary data to be written or read. It's aligned to 512 bytes in order to work with the O_DIRECT flag and SSDs. 
 * O_DIRECT is often used in databases when they implement their own caching/buffer management like in our case. The page size should also be a multiple of the OS' page size.
 */
template <PageSizeType SizeType>
struct alignas(PAGE_ALIGNMENT) Page {
  static_assert(bytes_for_size_type(SizeType) % PAGE_ALIGNMENT == 0,
                "SizeType needs to be a multiple of 512 for optimal SSD reads and writes");
  static_assert(bytes_for_size_type(SizeType) >= PAGE_ALIGNMENT,
                "SizeType needs to be larger than 512 for optimal SSD reads and writes");

  constexpr static std::size_t size() {
    return bytes_for_size_type(SizeType);
  }

  constexpr PageSizeType size_type() {
    return SizeType;
  }

  std::array<std::byte, bytes_for_size_type(SizeType)> _data;

  operator char*() {
    return reinterpret_cast<char*>(_data.data());
  }

  std::byte* data() {
    return _data.data();
  }

  friend bool operator==(const Page& p1, const Page& p2) noexcept {
    return p1._data == p2._data;
  }
};

using Page8KiB = Page<PageSizeType::KiB8>;
using Page16KiB = Page<PageSizeType::KiB16>;
using Page32KiB = Page<PageSizeType::KiB32>;
using Page64KiB = Page<PageSizeType::KiB64>;
using Page128KiB = Page<PageSizeType::KiB128>;

}  // namespace hyrise