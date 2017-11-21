#pragma once

#include <emmintrin.h>

#include <cstdint>

namespace opossum {

class SimdBp128Packing {
 public:
  static constexpr auto block_size = 128u;
  static constexpr auto blocks_in_meta_block = 16u;
  static constexpr auto meta_block_size = block_size * blocks_in_meta_block;

 public:
  static void write_meta_info(const uint8_t* in, __m128i* out);
  static void read_meta_info(const __m128i* in, uint8_t* out);

  static void pack_block(const uint32_t* _in, __m128i* out, const uint8_t bit_size);
  static void unpack_block(const __m128i* in, uint32_t* _out, const uint8_t bit_size);
};

}  // namespace opossum
