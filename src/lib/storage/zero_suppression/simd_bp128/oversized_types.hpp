#pragma once

#include <cstdint>

namespace opossum {

/**
 * @defgroup Oversized unsigned integer types
 *
 * Used to store aligned __m128i and __m256i in containers,
 * because __128i and __256i cannot be used as template parameters
 * with GCC v7.
 */
struct alignas(16) uint128_t {
  uint32_t data[4];
};

struct alignas(32) uint256_t {
  uint32_t data[8];
};
/**@}*/

}  // namespace opossum
