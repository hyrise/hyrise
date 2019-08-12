#include "run_length_segment.hpp"

#include <algorithm>

#include "resolve_type.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
RunLengthSegment<T>::RunLengthSegment(const std::shared_ptr<const pmr_vector<T>>& values,
                                      const std::shared_ptr<const pmr_vector<bool>>& null_values,
                                      const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions)
    : BaseEncodedSegment(data_type_from_type<T>()),
      _values{values},
      _null_values{null_values},
      _end_positions{end_positions} {}

template <typename T>
std::shared_ptr<const pmr_vector<T>> RunLengthSegment<T>::values() const {
  return _values;
}

template <typename T>
std::shared_ptr<const pmr_vector<bool>> RunLengthSegment<T>::null_values() const {
  return _null_values;
}

template <typename T>
std::shared_ptr<const pmr_vector<ChunkOffset>> RunLengthSegment<T>::end_positions() const {
  return _end_positions;
}

template <typename T>
const AllTypeVariant RunLengthSegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
size_t RunLengthSegment<T>::size() const {
  if (_end_positions->empty()) return 0u;
  return _end_positions->back() + 1u;
}

template <typename T>
std::shared_ptr<BaseSegment> RunLengthSegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_values = std::make_shared<pmr_vector<T>>(*_values, alloc);
  auto new_null_values = std::make_shared<pmr_vector<bool>>(*_null_values, alloc);
  auto new_end_positions = std::make_shared<pmr_vector<ChunkOffset>>(*_end_positions, alloc);

  return std::make_shared<RunLengthSegment<T>>(new_values, new_null_values, new_end_positions);
}

template <typename T>
size_t RunLengthSegment<T>::estimate_memory_usage() const {
  const auto null_values_size = _null_values->capacity() % CHAR_BIT ? _null_values->capacity() / CHAR_BIT + 1
                                                                    : _null_values->capacity() / CHAR_BIT;

  return sizeof(*this) + _values->capacity() * sizeof(typename decltype(_values)::element_type::value_type) +
         null_values_size +
         _end_positions->capacity() * sizeof(typename decltype(_end_positions)::element_type::value_type);
}

template <typename T>
EncodingType RunLengthSegment<T>::encoding_type() const {
  return EncodingType::RunLength;
}

template <typename T>
std::optional<CompressedVectorType> RunLengthSegment<T>::compressed_vector_type() const {
  return std::nullopt;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(RunLengthSegment);

}  // namespace opossum
