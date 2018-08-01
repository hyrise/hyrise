#include "frame_of_reference_column.hpp"

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T, typename U>
FrameOfReferenceColumn<T, U>::FrameOfReferenceColumn(pmr_vector<T> block_minima,
                                                     pmr_vector<bool> null_values,
                                                     std::unique_ptr<const BaseCompressedVector> offset_values)
    : BaseEncodedColumn{data_type_from_type<T>()},
      _block_minima{std::move(block_minima)},
      _null_values{std::move(null_values)},
      _offset_values{std::move(offset_values)},
      _decoder{_offset_values->create_base_decoder()} {}

template <typename T, typename U>
const pmr_vector<T>& FrameOfReferenceColumn<T, U>::block_minima() const {
  return _block_minima;
}

template <typename T, typename U>
const pmr_vector<bool>& FrameOfReferenceColumn<T, U>::null_values() const {
  return _null_values;
}

template <typename T, typename U>
const BaseCompressedVector& FrameOfReferenceColumn<T, U>::offset_values() const {
  return *_offset_values;
}

template <typename T, typename U>
const AllTypeVariant FrameOfReferenceColumn<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  if (_null_values[chunk_offset]) {
    return NULL_VALUE;
  }

  const auto minimum = _block_minima[chunk_offset / block_size];

  const auto value = static_cast<T>(_decoder->get(chunk_offset)) + minimum;

  return value;
}

template <typename T, typename U>
size_t FrameOfReferenceColumn<T, U>::size() const {
  return _offset_values->size();
}

template <typename T, typename U>
std::shared_ptr<BaseColumn> FrameOfReferenceColumn<T, U>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_block_minima = pmr_vector<T>{_block_minima, alloc};
  auto new_null_values = pmr_vector<bool>{_null_values, alloc};
  auto new_offset_values = _offset_values->copy_using_allocator(alloc);

  return std::allocate_shared<FrameOfReferenceColumn>(alloc, std::move(new_block_minima), std::move(new_null_values),
                                                      std::move(new_offset_values));
}

template <typename T, typename U>
size_t FrameOfReferenceColumn<T, U>::estimate_memory_usage() const {
  static const auto bits_per_byte = 8u;

  return sizeof(*this) + sizeof(T) * _block_minima.size() + _offset_values->data_size() +
         _null_values.size() / bits_per_byte;
}

template <typename T, typename U>
EncodingType FrameOfReferenceColumn<T, U>::encoding_type() const {
  return EncodingType::FrameOfReference;
}

template <typename T, typename U>
CompressedVectorType FrameOfReferenceColumn<T, U>::compressed_vector_type() const {
  return _offset_values->type();
}

template class FrameOfReferenceColumn<int32_t>;
template class FrameOfReferenceColumn<int64_t>;

}  // namespace opossum
