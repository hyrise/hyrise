#include "bitpacking_vector.hpp"


namespace opossum {

BitpackingVector::BitpackingVector(const CompactStaticVector& data) : _data{data} {}

size_t BitpackingVector::on_size() const { 
  return _data.size();
}

size_t BitpackingVector::on_data_size() const {
    return _data.bytes();
}

std::unique_ptr<BaseVectorDecompressor> BitpackingVector::on_create_base_decompressor() const {
  return std::make_unique<BitpackingDecompressor>(_data);
}

BitpackingDecompressor BitpackingVector::on_create_decompressor() const { 
  return BitpackingDecompressor(_data); 
}

BitpackingIterator BitpackingVector::on_begin() const { return BitpackingIterator(_data, 0u);}

BitpackingIterator BitpackingVector::on_end() const { return BitpackingIterator(_data, on_size()); }

std::unique_ptr<const BaseCompressedVector> BitpackingVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  
  CompactStaticVector copy(_data.b(), alloc);
  for (int i = 0; i < _data.size(); i++) {
    copy.push_back(_data.get(i));
  }

  return std::make_unique<BitpackingVector>(copy);
}

}  // namespace opossum
