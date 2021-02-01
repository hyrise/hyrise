#include "turboPFor_bitpacking_vector.hpp"

namespace opossum {

TurboPForBitpackingVector::TurboPForBitpackingVector(std::shared_ptr<pmr_vector<uint8_t>> data, size_t size, uint8_t b) : _data{data}, _size{size}, _b{b} {}

const std::shared_ptr<pmr_vector<uint8_t>> TurboPForBitpackingVector::data() const { return _data; }

size_t TurboPForBitpackingVector::on_size() const { return _size; }
size_t TurboPForBitpackingVector::on_data_size() const { return sizeof(uint8_t) * _data->size(); }

uint8_t TurboPForBitpackingVector::b() const { return _b; }

std::unique_ptr<BaseVectorDecompressor> TurboPForBitpackingVector::on_create_base_decompressor() const {
  return std::make_unique<TurboPForBitpackingDecompressor>(*this);
}

TurboPForBitpackingDecompressor TurboPForBitpackingVector::on_create_decompressor() const { return TurboPForBitpackingDecompressor(*this); }

TurboPForBitpackingIterator TurboPForBitpackingVector::on_begin() const { return TurboPForBitpackingIterator{on_create_decompressor(), 0u}; }

TurboPForBitpackingIterator TurboPForBitpackingVector::on_end() const { return TurboPForBitpackingIterator{on_create_decompressor(), _size}; }

std::unique_ptr<const BaseCompressedVector> TurboPForBitpackingVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = std::make_shared<pmr_vector<uint8_t>>(*_data, alloc);
  return std::make_unique<TurboPForBitpackingVector>(data_copy, _size, _b);
}

}  // namespace opossum
