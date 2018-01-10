#pragma once

#include <array>

#include "base_zero_suppression_encoder.hpp"
#include "oversized_types.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Encoder : public BaseZeroSuppressionEncoder {
 public:
  std::unique_ptr<BaseZeroSuppressionVector> encode(const pmr_vector<uint32_t>& vector,
                                                    const PolymorphicAllocator<size_t>& alloc) final;

 private:
  using Packing = SimdBp128Packing;

  void init(size_t size, const PolymorphicAllocator<size_t>& alloc);
  void append(uint32_t value);
  void finish();

  bool meta_block_complete();
  void pack_meta_block();
  void pack_incomplete_meta_block();

  std::array<uint8_t, Packing::blocks_in_meta_block> bits_needed_per_block();
  void write_meta_info(const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);
  void pack_blocks(const uint8_t num_blocks, const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);

 private:
  std::unique_ptr<pmr_vector<uint128_t>> _data;
  size_t _data_index;

  std::array<uint32_t, Packing::meta_block_size> _pending_meta_block;
  size_t _meta_block_index;

  size_t _size;
};
}  // namespace opossum
