#include "bitpacking_vector.hpp"

#include "bitpacking_decompressor.hpp"
#include "bitpacking_iterator.hpp"

namespace opossum {

BitPackingVector::BitPackingVector(pmr_compact_vector<uint32_t> data) : _data{std::move(data)} {}

const pmr_compact_vector<uint32_t>& BitPackingVector::data() const { return _data; }

size_t BitPackingVector::on_size() const { return _data.size(); }
size_t BitPackingVector::on_data_size() const { return _data.bytes(); }

std::unique_ptr<BaseVectorDecompressor> BitPackingVector::on_create_base_decompressor() const {
  return std::make_unique<BitPackingDecompressor>(_data);
}

BitPackingDecompressor BitPackingVector::on_create_decompressor() const { return BitPackingDecompressor(_data); }

BitPackingIterator BitPackingVector::on_begin() const { return BitPackingIterator(_data, 0u); }

BitPackingIterator BitPackingVector::on_end() const { return BitPackingIterator(_data, _data.size()); }

std::unique_ptr<const BaseCompressedVector> BitPackingVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = pmr_compact_vector<uint32_t>(_data.bits(), _data.size(), alloc);
  // zero initialize the compactvectors memory, see bitpacking_compressor.cpp
  std::fill_n(data_copy.get(), data_copy.bytes() / 8, 0);
  std::copy(_data.cbegin(), _data.cend(), data_copy.begin());

  return std::make_unique<BitPackingVector>(std::move(data_copy));
}

}  // namespace opossum
