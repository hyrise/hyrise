#pragma once

#include "bitpacking_decompressor.hpp"
#include "bitpacking_iterator.hpp"
#include "bitpacking_vector_type.hpp"
#include "compact_vector.hpp"
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
class BitpackingVector : public CompressedVector<BitpackingVector> {
 public:
  explicit BitpackingVector(const pmr_bitpacking_vector<uint32_t>& data);
  ~BitpackingVector() override = default;

  const pmr_bitpacking_vector<uint32_t>& data() const;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  BitpackingDecompressor on_create_decompressor() const;

  BitpackingIterator on_begin() const;
  BitpackingIterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:
  friend class BitpackingDecompressor;

  const pmr_bitpacking_vector<uint32_t> _data;
};

}  // namespace opossum
