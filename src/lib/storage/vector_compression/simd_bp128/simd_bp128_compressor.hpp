#pragma once

#include <array>

#include "storage/vector_compression/base_vector_compressor.hpp"

#include "oversized_types.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Compresses a vector using SIMD-BP128
 */
class SimdBp128Compressor : public BaseVectorCompressor {
 public:
  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc,
                                                       const UncompressedVectorInfo& meta_info = {}) final;

  std::unique_ptr<BaseVectorCompressor> create_new() const final;

 private:
  using Packing = SimdBp128Packing;

  void _init(size_t size, const PolymorphicAllocator<size_t>& alloc);
  void _append(uint32_t value);
  void _finish();

  bool _meta_block_complete();
  void _pack_meta_block();
  void _pack_incomplete_meta_block();

  std::array<uint8_t, Packing::blocks_in_meta_block> _bits_needed_per_block();
  void _write_meta_info(const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);
  void _pack_blocks(const uint8_t num_blocks, const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed);

 private:
  std::unique_ptr<pmr_vector<uint128_t>> _data;
  size_t _data_index;

  alignas(16) std::array<uint32_t, Packing::meta_block_size> _pending_meta_block;
  size_t _meta_block_index;

  size_t _size;
};
}  // namespace opossum
