#include "fixed_string_dictionary_segment.hpp"

#include <algorithm>
#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
FixedStringDictionarySegment<T>::FixedStringDictionarySegment(
    const std::shared_ptr<const FixedStringVector>& dictionary,
    const std::shared_ptr<const BaseCompressedVector>& attribute_vector, const ValueID null_value_id)
    : BaseDictionarySegment(data_type_from_type<pmr_string>()),
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _null_value_id{null_value_id},
      _decompressor{_attribute_vector->create_base_decompressor()} {}

template <typename T>
AllTypeVariant FixedStringDictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
std::optional<T> FixedStringDictionarySegment<T>::get_typed_value(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset < size(), "ChunkOffset out of bounds.");

  const auto value_id = _decompressor->get(chunk_offset);
  if (value_id == _null_value_id) {
    return std::nullopt;
  }
  return _dictionary->get_string_at(value_id);
}

template <typename T>
std::shared_ptr<const pmr_vector<pmr_string>> FixedStringDictionarySegment<T>::dictionary() const {
  return _dictionary->dictionary();
}

template <typename T>
std::shared_ptr<const FixedStringVector> FixedStringDictionarySegment<T>::fixed_string_dictionary() const {
  return _dictionary;
}

template <typename T>
ChunkOffset FixedStringDictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<BaseSegment> FixedStringDictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector_ptr = _attribute_vector->copy_using_allocator(alloc);
  auto new_attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(new_attribute_vector_ptr));
  auto new_dictionary = FixedStringVector(*_dictionary);
  auto new_dictionary_ptr = std::allocate_shared<FixedStringVector>(alloc, std::move(new_dictionary));
  return std::allocate_shared<FixedStringDictionarySegment<T>>(alloc, new_dictionary_ptr, new_attribute_vector_sptr,
                                                               _null_value_id);
}

template <typename T>
size_t FixedStringDictionarySegment<T>::estimate_memory_usage() const {
  return sizeof(*this) + _dictionary->data_size() + _attribute_vector->data_size();
}

template <typename T>
std::optional<CompressedVectorType> FixedStringDictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
EncodingType FixedStringDictionarySegment<T>::encoding_type() const {
  return EncodingType::FixedStringDictionary;
}

template <typename T>
ValueID FixedStringDictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = boost::get<pmr_string>(value);

  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T>
ValueID FixedStringDictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = boost::get<pmr_string>(value);

  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T>
AllTypeVariant FixedStringDictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary->size(), "ValueID out of bounds");
  return _dictionary->get_string_at(value_id);
}

template <typename T>
ValueID::base_type FixedStringDictionarySegment<T>::unique_values_count() const {
  return static_cast<ValueID::base_type>(_dictionary->size());
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> FixedStringDictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID FixedStringDictionarySegment<T>::null_value_id() const {
  return _null_value_id;
}

template class FixedStringDictionarySegment<pmr_string>;

}  // namespace opossum
