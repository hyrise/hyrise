#pragma once

#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief Page wraps binary data to be written or read. It's aligned to 512 bytes in order to work with the O_DIRECT flag and SSDs. 
 * O_DIRECT is often used in databases when they implement their own caching/buffer management like in our case.
 */
template <PageSizeType SizeType>
struct alignas(512) Page {
  static_assert(static_cast<std::size_t>(SizeType) % 512 == 0,
                "SizeType needs to be a multiple of 512 for optimal SSD reads and writes");
  static_assert(static_cast<std::size_t>(SizeType) >= 512,
                "SizeType needs to be larger than 512 for optimal SSD reads and writes");

  constexpr static std::size_t Size() {
    return static_cast<std::size_t>(SizeType);
  }

  std::array<std::byte, static_cast<std::size_t>(SizeType)> _data;

  std::byte* data() {
    return _data.data();
  }
};

using Page32KiB = Page<PageSizeType::KiB32>;

}  // namespace hyrise