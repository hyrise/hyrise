#pragma once

#include <cstdint>

#include "oversized_types.hpp"

namespace opossum {

/**
 * @brief Implementation of the bit-packing algorithm SIMD-BP128
 *
 * Paper: https://dl.acm.org/citation.cfm?id=2904570
 * Reference Implementation: https://github.com/lemire/FastPFor/blob/master/src/simdunalignedbitpacking.cpp
 *
 * Unlike the reference implementation, this implementation uses templates and is thus much shorter.
 */
class SimdBp128Packing {
 public:
  static constexpr auto block_size = 128u;
  static constexpr auto blocks_in_meta_block = 16u;
  static constexpr auto meta_block_size = block_size * blocks_in_meta_block;

 public:
  static void write_meta_info(const uint8_t* in, uint128_t* out);
  static void read_meta_info(const uint128_t* in, uint8_t* out);

  static void pack_block(const uint32_t* in, uint128_t* out, const uint8_t bit_size);
  static void unpack_block(const uint128_t* in, uint32_t* out, const uint8_t bit_size);
};

}  // namespace opossum
