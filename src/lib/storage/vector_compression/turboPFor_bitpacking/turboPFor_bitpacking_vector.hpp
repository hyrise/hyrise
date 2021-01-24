#pragma once

#include <memory>

#include "turboPFor_bitpacking_decompressor.hpp"
#include "turboPFor_bitpacking_iterator.hpp"

#include "storage/vector_compression/base_compressed_vector.hpp"
#include "types.hpp"

namespace opossum {

/**
 * @brief Stores values as bitpacking in uint8_t vector
 *
 */
class TurboPFORBitpackingVector : public CompressedVector<TurboPFORBitpackingVector> {

 public:
  explicit TurboPFORBitpackingVector(pmr_vector<uint8_t> data, size_t size, uint8_t b) : _data{std::move(data)}, _size{size}, _b{b} {}

  const pmr_vector<uint8_t>& data() const { return _data; }

 public:
  size_t on_size() const { return _size; }
  size_t on_data_size() const { return sizeof(uint8_t) * _data.size(); }

  auto on_create_base_decompressor() const {
    return std::make_unique<TurboPForBitpackingDecompressor>(_data, _size, _b);
  }

  auto on_create_decompressor() const { return TurboPForBitpackingDecompressor(_data, _size, _b); }
  
  TurboPForBitpackingIterator on_begin() const { return TurboPForBitpackingIterator{on_create_decompressor(), 0u}; }

  TurboPForBitpackingIterator on_end() const { return TurboPForBitpackingIterator{on_create_decompressor(), _size}; }

  std::unique_ptr<const BaseCompressedVector> on_copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
    auto data_copy = pmr_vector<uint8_t>{_data, alloc};
    return std::make_unique<TurboPFORBitpackingVector>(std::move(data_copy), _size, _b);
  }

 private:
  const pmr_vector<uint8_t> _data;
  const size_t _size;
  const uint8_t _b;
};

}  // namespace opossum
