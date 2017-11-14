#pragma once

#include <emmintrin.h>

#include <array>

#include "base_attribute_encoder.hpp"


namespace opossum {

class SimdBp128Encoder : public BaseAttributeEncoder {
 public:
  void init(size_t size) final;
  void append(uint32_t value) final;
  void finish() final;
  std::unique_ptr<BaseEncodedVector> get_vector() final;

 public:
  static constexpr auto block_size = 128u;
  static constexpr auto blocks_in_meta_block = 16u;
  static constexpr auto meta_block_size = block_size * blocks_in_meta_block;

 private:
  bool meta_block_complete();
  void pack_meta_block();
  void pack_incomplete_meta_block();

  std::array<uint8_t, blocks_in_meta_block> bits_needed_per_block();
  void write_meta_info(const std::array<uint8_t, blocks_in_meta_block>& bits_needed);
  void pack_blocks(const uint8_t num_blocks, const std::array<uint8_t, blocks_in_meta_block>& bits_needed);
  void pack_block(const uint32_t* _in, __m128i* out, const uint8_t bit_size);

 private:
  pmr_vector<__m128i> _data;
  size_t _data_index;

  std::array<uint32_t, meta_block_size> _pending_meta_block;
  size_t _meta_block_index;

  size_t _size;
};
}  // namespace opossum
