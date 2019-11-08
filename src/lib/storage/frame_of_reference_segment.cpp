#include "frame_of_reference_segment.hpp"

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T, typename U>
FrameOfReferenceSegment<T, U>::FrameOfReferenceSegment(pmr_vector<T> block_minima, pmr_vector<bool> null_values,
                                                       std::unique_ptr<const BaseCompressedVector> offset_values)
    : BaseEncodedSegment{data_type_from_type<T>()},
      _block_minima{std::move(block_minima)},
      _null_values{std::move(null_values)},
      _offset_values{std::move(offset_values)},
      _decompressor{_offset_values->create_base_decompressor()} {}

template <typename T, typename U>
const pmr_vector<T>& FrameOfReferenceSegment<T, U>::block_minima() const {
  return _block_minima;
}

template <typename T, typename U>
const pmr_vector<bool>& FrameOfReferenceSegment<T, U>::null_values() const {
  return _null_values;
}

template <typename T, typename U>
const BaseCompressedVector& FrameOfReferenceSegment<T, U>::offset_values() const {
  return *_offset_values;
}

template <typename T, typename U>
AllTypeVariant FrameOfReferenceSegment<T, U>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset < size(), "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T, typename U>
ChunkOffset FrameOfReferenceSegment<T, U>::size() const {
  return static_cast<ChunkOffset>(_offset_values->size());
}

template <typename T, typename U>
std::shared_ptr<BaseSegment> FrameOfReferenceSegment<T, U>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_block_minima = pmr_vector<T>{_block_minima, alloc};
  auto new_null_values = pmr_vector<bool>{_null_values, alloc};
  auto new_offset_values = _offset_values->copy_using_allocator(alloc);

  return std::allocate_shared<FrameOfReferenceSegment>(alloc, std::move(new_block_minima), std::move(new_null_values),
                                                       std::move(new_offset_values));
}

template <typename T, typename U>
size_t FrameOfReferenceSegment<T, U>::estimate_memory_usage() const {
  static const auto bits_per_byte = 8u;

  return sizeof(*this) + sizeof(T) * _block_minima.size() + _offset_values->data_size() +
         _null_values.size() / bits_per_byte;
}

template <typename T, typename U>
EncodingType FrameOfReferenceSegment<T, U>::encoding_type() const {
  return EncodingType::FrameOfReference;
}

template <typename T, typename U>
std::optional<CompressedVectorType> FrameOfReferenceSegment<T, U>::compressed_vector_type() const {
  return _offset_values->type();
}

template class FrameOfReferenceSegment<int32_t>;
// int64_t disabled for now, as vector compression cannot handle 64 bit values - also in reference_segment_iterable.hpp
// template class FrameOfReferenceSegment<int64_t>;

}  // namespace opossum
