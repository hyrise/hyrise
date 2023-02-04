#pragma once

namespace hyrise {

constexpr auto PAGE_SIZE = 131072;

static_assert(PAGE_SIZE % 512 == 0, "PAGE_SIZE needs to be a multiple of 512 for optimal SSD reads and writes");
static_assert(PAGE_SIZE >= 512, "PAGE_SIZE needs to be larger than 512 for optimal SSD reads and writes");

/**
 * @brief Page wraps binary data to be written or read. It's aligned to 512 bytes in order to work with the O_DIRECT flag and SSDs. 
 * O_DIRECT is often used in databases when they implement their own caching/buffer management like in our case.
 */
struct alignas(512) Page {
  std::array<std::byte, PAGE_SIZE> data;
};

}  // namespace hyrise