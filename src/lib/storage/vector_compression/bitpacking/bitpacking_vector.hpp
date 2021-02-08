#pragma once

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "compact_vector.hpp"

#include "vector_types.hpp"

#include "bitpacking_iterator.hpp"
#include "bitpacking_decompressor.hpp"
#include "compact_static_vector.hpp"

namespace opossum {

class BitpackingDecompressor;
class BitpackingIterator; 

class BitpackingVector : public CompressedVector<BitpackingVector> {
 public:
  explicit BitpackingVector(const CompactStaticVector& data);

  ~BitpackingVector() override = default;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  BitpackingDecompressor on_create_decompressor() const;

  BitpackingIterator on_begin() const;
  BitpackingIterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:

  CompactStaticVector _data;

  friend class BitpackingDecompressor;
  
  };
}  // namespace opossum
