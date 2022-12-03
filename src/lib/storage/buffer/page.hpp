#pragma once
#include "types.hpp"

namespace hyrise {

constexpr auto PAGE_SIZE = 512;

/**
 * @brief Page wraps binary data to be written or read. It's aligned to 512 bytes in order to work with the O_DIRECT flag. 
 * O_DIRECT is often used in databases when they implement their own caching/buffer management like in our case.
 */
struct alignas(512) Page {
  std::array<std::byte, PAGE_SIZE> data;
};

}  // namespace hyrise