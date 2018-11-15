#pragma once

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "oversized_types.hpp"
#include "simd_bp128_decompressor.hpp"
#include "simd_bp128_iterator.hpp"

#include "types.hpp"

namespace opossum {

/**
 * @brief Bit-packed vector with varying bit length
 *
 * Values are compressed in blocks of 128 integers. Each block has its own bit-length.
 * Sixteen blocks combine to form a meta block of 2048 values. Bit-length information
 * are stored per meta block in 128 bit (8 bit for each bit length) in front of the
 * sixteen compressed blocks.
 *
 * @see SimdBp128Packing for more information
 */
class SimdBp128Vector : public CompressedVector<SimdBp128Vector> {
 public:
  explicit SimdBp128Vector(pmr_vector<uint128_t> vector, size_t size);
  ~SimdBp128Vector() = default;

  const pmr_vector<uint128_t>& data() const;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  std::unique_ptr<SimdBp128Decompressor> on_create_decompressor() const;

  SimdBp128Iterator on_begin() const;
  SimdBp128Iterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:
  friend class SimdBp128Decompressor;

  const pmr_vector<uint128_t> _data;
  const size_t _size;
};
}  // namespace opossum
