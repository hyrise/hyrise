#pragma once

#include "storage/vector_compression/base_compressed_vector.hpp"

#include "turboPFor_bitpacking_decompressor.hpp"
#include "turboPFor_bitpacking_iterator.hpp"

#include "types.hpp"

namespace opossum {

class TurboPForBitpackingVector : public CompressedVector<TurboPForBitpackingVector> {
 public:
  explicit TurboPForBitpackingVector(pmr_vector<uint8_t> vector, size_t size, uint8_t b);
  ~TurboPForBitpackingVector() override = default;

  const pmr_vector<uint8_t>& data() const;

  size_t on_size() const;
  size_t on_data_size() const;

  std::unique_ptr<BaseVectorDecompressor> on_create_base_decompressor() const;
  TurboPForBitpackingDecompressor on_create_decompressor() const;

  TurboPForBitpackingIterator on_begin() const;
  TurboPForBitpackingIterator on_end() const;

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const;

 private:
  friend class TurboPForBitpackingDecompressor;

  const pmr_vector<uint8_t> _data;
  const size_t _size;
  const uint8_t _b;
};

}  // namespace opossum
