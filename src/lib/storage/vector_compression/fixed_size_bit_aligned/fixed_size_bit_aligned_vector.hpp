#pragma once

#include "compact_vector.hpp"
#include "fixed_size_bit_aligned_decompressor.hpp"
#include "fixed_size_bit_aligned_iterator.hpp"
#include "fixed_size_bit_aligned_vector_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"

namespace opossum {

/**
 * @brief Bit-packed vector with fixed bit length
 *
 * Bit-aligned Null Suppression.
 * All values of the sequences are compressed with the same bit length, which is determined by the bits required to 
 * represent the maximum value of the sequence. The compression ratio is worse than that of SimdBp128Vector 
 * because the the bit-width is fixed for the whole sequence and not determined per value. The decoding runtime is 
 * vastly better than SimdBp128Vector and competitive to FixedSizeByteAlignedVector.
 * 
 *
 * @see SimdBp128Packing for more information
 */
class FixedSizeBitAlignedVector : public CompressedVector<FixedSizeBitAlignedVector> {
 public:
  explicit FixedSizeBitAlignedVector(const pmr_compact_vector<uint32_t>& data);
  ~FixedSizeBitAlignedVector() override = default;

  const pmr_compact_vector<uint32_t>& data() const;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  FixedSizeBitAlignedDecompressor on_create_decompressor() const;

  FixedSizeBitAlignedIterator on_begin() const;
  FixedSizeBitAlignedIterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:
  friend class FixedSizeBitAlignedDecompressor;

  const pmr_compact_vector<uint32_t> _data;
};

}  // namespace opossum
