#include "bitpacking_vector.hpp"
#include "bitpacking_iterator.hpp"
#include "bitpacking_decompressor.hpp"

namespace opossum {

BitpackingVector::BitpackingVector(const pmr_bitpacking_vector<uint32_t>& data) : _data{data} {}

const pmr_bitpacking_vector<uint32_t>& BitpackingVector::data() const { return _data; }

size_t BitpackingVector::on_size() const { return _data.size(); }
size_t BitpackingVector::on_data_size() const { return _data.bytes(); }

std::unique_ptr<BaseVectorDecompressor> BitpackingVector::on_create_base_decompressor() const {
  return std::make_unique<BitpackingDecompressor>(_data);
}

BitpackingDecompressor BitpackingVector::on_create_decompressor() const { return BitpackingDecompressor(_data); }

BitpackingIterator BitpackingVector::on_begin() const { return BitpackingIterator(_data, 0u);}

BitpackingIterator BitpackingVector::on_end() const { return BitpackingIterator(_data, _data.size()); }

std::unique_ptr<const BaseCompressedVector> BitpackingVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = pmr_bitpacking_vector<uint32_t>(_data.used_bits(), _data.size(), alloc);
  for (int i = 0; i < _data.size(); i++) {
    data_copy[i] = _data[i];
  }
  return std::make_unique<BitpackingVector>(data_copy);
}

}  // namespace opossum
