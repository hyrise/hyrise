#include "frame_of_reference_column.hpp"

#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

#include "storage/vector_compression/base_compressed_vector.hpp"

namespace opossum {

template <typename T>
FrameOfReferenceColumn<T>::FrameOfReferenceColumn(std::shared_ptr<const pmr_vector<T>> reference_frames,
                                                  std::shared_ptr<const BaseCompressedVector> offset_values,
                                                  std::shared_ptr<const pmr_vector<bool>> null_values)
    : _reference_frames{reference_frames},
      _offset_values{offset_values},
      _null_values{null_values} {}

template <typename T>
std::shared_ptr<const pmr_vector<T>> FrameOfReferenceColumn<T>::reference_frames() const {
  return _reference_frames;
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> FrameOfReferenceColumn<T>::offset_values() const {
  return _offset_values;
}

template <typename T>
std::shared_ptr<const pmr_vector<bool>> FrameOfReferenceColumn<T>::null_values() const {
  return _null_values;
}

template <typename T>
const AllTypeVariant FrameOfReferenceColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  if ((*_null_values)[chunk_offset]) {
    return NULL_VALUE;
  }

  const auto reference_frame = (*_reference_frames)[chunk_offset / frame_size];

  auto decoder = _offset_values->create_base_decoder();
  const auto value = static_cast<T>(decoder->get(chunk_offset)) + reference_frame;

  return value;
}

template <typename T>
size_t FrameOfReferenceColumn<T>::size() const { return _offset_values->size(); }

template <typename T>
std::shared_ptr<BaseColumn> FrameOfReferenceColumn<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_reference_frames = std::allocate_shared<pmr_vector<T>>(alloc, *_reference_frames, alloc);
  auto new_offset_values = _offset_values->copy_using_allocator(alloc);
  auto new_null_values = std::allocate_shared<pmr_vector<bool>>(alloc, *_null_values, alloc);

  return std::allocate_shared<FrameOfReferenceColumn>(alloc, new_reference_frames, new_offset_values, new_null_values);
}

template <typename T>
size_t FrameOfReferenceColumn<T>::estimate_memory_usage() const {
  static const auto bits_per_byte = 8u;

  return sizeof(*this) + sizeof(typename decltype(_reference_frames)::element_type) +
      _offset_values->data_size() + _null_values->size() / bits_per_byte;
}

template <typename T>
EncodingType FrameOfReferenceColumn<T>::encoding_type() const { return EncodingType::FrameOfReference; }

template <typename T>
CompressedVectorType FrameOfReferenceColumn<T>::compressed_vector_type() const { return _offset_values->type(); }

template class FrameOfReferenceColumn<int32_t>;
template class FrameOfReferenceColumn<int64_t>;

}  // namespace opossum
