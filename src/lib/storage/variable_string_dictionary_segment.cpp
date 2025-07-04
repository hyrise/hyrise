#include "variable_string_dictionary_segment.hpp"

#include <cmath>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

#include "all_type_variant.hpp"
#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/base_dictionary_segment.hpp"
#include "storage/encoding_type.hpp"
#include "storage/segment_access_counter.hpp"
#include "storage/variable_string_dictionary/variable_string_vector.hpp"
#include "storage/vector_compression/compressed_vector_type.hpp"
#include "types.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace hyrise {

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
VariableStringDictionarySegment<T>::VariableStringDictionarySegment(
    pmr_vector<char>&& dictionary, std::unique_ptr<const BaseCompressedVector>&& attribute_vector,
    pmr_vector<uint32_t>&& offset_vector)
    : BaseDictionarySegment(data_type_from_type<pmr_string>()),
      _dictionary{std::move(dictionary)},
      _attribute_vector{std::move(attribute_vector)},
      _decompressor{_attribute_vector->create_base_decompressor()},
      _offset_vector{std::move(offset_vector)} {
  // NULL is represented by _offset_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a VariableStringDictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(_offset_vector.size() < std::numeric_limits<ValueID::base_type>::max(),
         "Input segment too large to store as VariableStringDictionarySegment.");
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
const pmr_vector<char>& VariableStringDictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
VariableStringVector VariableStringDictionarySegment<T>::variable_string_dictionary() const {
  return VariableStringVector(_dictionary, _offset_vector);
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
AllTypeVariant VariableStringDictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used.");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value = get_typed_value(chunk_offset);
  return value ? value.value() : NULL_VALUE;
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
ChunkOffset VariableStringDictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
std::shared_ptr<AbstractSegment> VariableStringDictionarySegment<T>::copy_using_memory_resource(
    MemoryResource& memory_resource) const {
  auto copy = std::make_shared<VariableStringDictionarySegment>(pmr_vector<char>(_dictionary, &memory_resource),
                                                                _attribute_vector->copy_using_memory_resource(memory_resource),
                                                                pmr_vector<uint32_t>(_offset_vector, &memory_resource));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
size_t VariableStringDictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode /*mode*/) const {
  using OffsetVectorType = typename decltype(_offset_vector)::value_type;
  return _attribute_vector->data_size() + _dictionary.capacity() + _offset_vector.capacity() * sizeof(OffsetVectorType);
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
std::optional<CompressedVectorType> VariableStringDictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
EncodingType VariableStringDictionarySegment<T>::encoding_type() const {
  return EncodingType::VariableStringDictionary;
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
ValueID VariableStringDictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "NULL value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_offset_vector.size())));

  const auto typed_value = boost::get<pmr_string>(value);

  auto it = std::lower_bound(_offset_vector.begin(), _offset_vector.end(), typed_value,
                             [this](const auto& offset, const auto& to_find) {
                               const auto value = std::string_view{_dictionary.data() + offset};
                               return value < to_find;
                             });
  if (it == _offset_vector.end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_offset_vector.begin(), it))};
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
ValueID VariableStringDictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "NULL value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_offset_vector.size())));

  const auto typed_value = boost::get<pmr_string>(value);

  auto it = std::upper_bound(_offset_vector.begin(), _offset_vector.end(), typed_value,
                             [this](const auto& to_find, const auto& offset) {
                               const auto value = std::string_view{_dictionary.data() + offset};
                               return value > to_find;
                             });
  if (it == _offset_vector.end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_offset_vector.begin(), it))};
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
AllTypeVariant VariableStringDictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  // We do not increase SegmentAccessCounter in true case because we do not access the dictionary.
  return value_id == null_value_id() ? NULL_VALUE : typed_value_of_value_id(value_id);
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
pmr_string VariableStringDictionarySegment<T>::typed_value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _offset_vector.size(), "ValueID out of bounds.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;

  return pmr_string{get_string(_offset_vector, _dictionary, value_id)};
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
ValueID::base_type VariableStringDictionarySegment<T>::unique_values_count() const {
  return _offset_vector.size();
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
const std::unique_ptr<const BaseCompressedVector>& VariableStringDictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
ValueID VariableStringDictionarySegment<T>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_offset_vector.size())};
}

template <typename T>
  requires(std::is_same_v<T, pmr_string>)
const pmr_vector<uint32_t>& VariableStringDictionarySegment<T>::offset_vector() const {
  return _offset_vector;
}

template class VariableStringDictionarySegment<pmr_string>;

}  // namespace hyrise
