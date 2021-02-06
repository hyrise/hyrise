#pragma once

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "bitpacking_decompressor.hpp"
#include "bitpacking_iterator.hpp"

#include "compact_vector.hpp"

#include "vector_types.hpp"

namespace opossum {

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
