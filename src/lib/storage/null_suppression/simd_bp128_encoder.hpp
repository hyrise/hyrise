#pragma once

#include <emmintrin.h>

#include <array>

#include "base_ns_encoder.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"


namespace opossum {

class SimdBp128Encoder : public BaseNsEncoder {
 public:
  void init(size_t size) final;
  void append(uint32_t value) final;
  void finish() final;
  std::unique_ptr<BaseNsVector> get_vector() final;

 private:
  using Packing = SimdBp128Packing;

  bool meta_block_complete();
  void pack_meta_block();
  void pack_incomplete_meta_block();

  std::array<uint8_t, Packing::blocks_in_meta_block> bits_needed_per_block();
  void write_meta_info(const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);
  void pack_blocks(const uint8_t num_blocks, const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);

 private:
  pmr_vector<__m128i> _data;
  size_t _data_index;

  std::array<uint32_t, Packing::meta_block_size> _pending_meta_block;
  size_t _meta_block_index;

  size_t _size;
};
}  // namespace opossum
