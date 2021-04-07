#include "fixed_size_bit_aligned_vector.hpp"

#include "fixed_size_bit_aligned_decompressor.hpp"
#include "fixed_size_bit_aligned_iterator.hpp"

namespace opossum {

FixedSizeBitAlignedVector::FixedSizeBitAlignedVector(const pmr_compact_vector<uint32_t>& data) : _data{data} {}
FixedSizeBitAlignedVector::FixedSizeBitAlignedVector(pmr_compact_vector<uint32_t>&& data) : _data{std::move(data)} {}

const pmr_compact_vector<uint32_t>& FixedSizeBitAlignedVector::data() const { return _data; }

size_t FixedSizeBitAlignedVector::on_size() const { return _data.size(); }
size_t FixedSizeBitAlignedVector::on_data_size() const { return _data.bytes(); }

std::unique_ptr<BaseVectorDecompressor> FixedSizeBitAlignedVector::on_create_base_decompressor() const {
  return std::make_unique<FixedSizeBitAlignedDecompressor>(_data);
}

FixedSizeBitAlignedDecompressor FixedSizeBitAlignedVector::on_create_decompressor() const {
  return FixedSizeBitAlignedDecompressor(_data);
}

FixedSizeBitAlignedIterator FixedSizeBitAlignedVector::on_begin() const {
  return FixedSizeBitAlignedIterator(_data, 0u);
}

FixedSizeBitAlignedIterator FixedSizeBitAlignedVector::on_end() const {
  return FixedSizeBitAlignedIterator(_data, _data.size());
}

std::unique_ptr<const BaseCompressedVector> FixedSizeBitAlignedVector::on_copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto data_copy = pmr_compact_vector<uint32_t>(_data.used_bits(), _data.size(), alloc);
  std::copy(_data.cbegin(), _data.cend(), data_copy.begin());

  return std::make_unique<FixedSizeBitAlignedVector>(std::move(data_copy));
}

}  // namespace opossum
