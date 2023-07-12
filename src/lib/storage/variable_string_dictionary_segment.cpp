#include "variable_string_dictionary_segment.hpp"
#include <numeric>

#include "resolve_type.hpp"

namespace hyrise {

template <typename T>
VariableStringDictionarySegment<T>::VariableStringDictionarySegment(
    const std::shared_ptr<const pmr_vector<char>>& dictionary,
    const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : AbstractEncodedSegment(data_type_from_type<pmr_string>()),
      _klotz{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{attribute_vector->create_base_decompressor()} {}

template <typename T>
AllTypeVariant VariableStringDictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  // access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto value = get_typed_value(chunk_offset);
  return value ? value.value() : NULL_VALUE;
}

template <typename T>
std::optional<CompressedVectorType> VariableStringDictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
ChunkOffset VariableStringDictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<AbstractSegment> VariableStringDictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<char>>(*_klotz, alloc);
  auto copy =
      std::make_shared<VariableStringDictionarySegment>(std::move(new_dictionary), std::move(new_attribute_vector));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
size_t VariableStringDictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode /*mode*/) const {
  return _attribute_vector->data_size() + _klotz->capacity();
}

template <typename T>
EncodingType VariableStringDictionarySegment<T>::encoding_type() const {
  return EncodingType::VariableStringDictionary;
}

template <typename T>
const std::shared_ptr<const BaseCompressedVector>& VariableStringDictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const std::shared_ptr<const pmr_vector<char>>& VariableStringDictionarySegment<T>::klotz() const {
  return _klotz;
}

template class VariableStringDictionarySegment<pmr_string>;

}  // namespace hyrise
