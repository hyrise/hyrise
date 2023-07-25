#include "variable_string_dictionary_segment.hpp"
#include <numeric>

#include "resolve_type.hpp"

namespace hyrise {

template <typename T>
VariableStringDictionarySegment<T>::VariableStringDictionarySegment(
    const std::shared_ptr<const pmr_vector<char>>& dictionary,
    const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
    const std::shared_ptr<const pmr_vector<uint32_t>>& offset_vector)
    : BaseDictionarySegment(data_type_from_type<pmr_string>()),
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{attribute_vector->create_base_decompressor()},
      _offset_vector{offset_vector} {
  // NULL is represented by _offset_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a VariableStringDictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(_offset_vector->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T>
std::shared_ptr<const pmr_vector<char>> VariableStringDictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<VariableStringVector> VariableStringDictionarySegment<T>::variable_string_dictionary() const {
  return std::make_shared<VariableStringVector>(dictionary(), _offset_vector->size());
}

template <typename T>
AllTypeVariant VariableStringDictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  // access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto value = get_typed_value(chunk_offset);
  return value ? value.value() : NULL_VALUE;
}

template <typename T>
ChunkOffset VariableStringDictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<AbstractSegment> VariableStringDictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<char>>(*_dictionary, alloc);
  auto new_offset = std::make_shared<pmr_vector<uint32_t>>(*_offset_vector, alloc);
  auto copy = std::make_shared<VariableStringDictionarySegment>(std::move(new_dictionary),
                                                                std::move(new_attribute_vector), std::move(new_offset));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
size_t VariableStringDictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode /*mode*/) const {
  using OffsetVectorType = typename std::decay<decltype(*_offset_vector->begin())>::type;
  return _attribute_vector->data_size() + _dictionary->capacity() +
         _offset_vector->capacity() * sizeof(OffsetVectorType);
}

template <typename T>
std::optional<CompressedVectorType> VariableStringDictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
EncodingType VariableStringDictionarySegment<T>::encoding_type() const {
  return EncodingType::VariableStringDictionary;
}

template <typename T>
ValueID VariableStringDictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  //  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
  //      static_cast<uint64_t>(std::ceil(std::log2(_offset_vector->size())));
  const auto typed_value = boost::get<pmr_string>(value);

  auto args = std::vector<ValueID>(_offset_vector->size());
  std::iota(args.begin(), args.end(), 0);
  auto it = std::lower_bound(
      args.cbegin(), args.cend(), typed_value,
      [this](const ValueID valueId, const auto to_find) { return typed_value_of_value_id(valueId) < to_find; });
  if (it == args.cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(args.cbegin(), it))};
}

template <typename T>
ValueID VariableStringDictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  //  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
  //    static_cast<uint64_t>(std::ceil(std::log2(_offset_vector->size())));
  const auto typed_value = boost::get<pmr_string>(value);

  auto args = std::vector<ValueID>(_offset_vector->size());
  std::iota(args.begin(), args.end(), 0);
  auto it = std::upper_bound(
      args.cbegin(), args.cend(), typed_value,
      [this](const auto to_find, const ValueID valueID) { return to_find < typed_value_of_value_id(valueID); });
  if (it == args.cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(args.cbegin(), it))};
}

template <typename T>
AllTypeVariant VariableStringDictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  return value_id == null_value_id() ? NULL_VALUE : typed_value_of_value_id(value_id);
}

template <typename T>
pmr_string VariableStringDictionarySegment<T>::typed_value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _offset_vector->size(), "ValueID out of bounds");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  return pmr_string{_dictionary->data() + _offset_vector->operator[](value_id)};
}

template <typename T>
ValueID::base_type VariableStringDictionarySegment<T>::unique_values_count() const {
  return _offset_vector->size();
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> VariableStringDictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID VariableStringDictionarySegment<T>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_offset_vector->size())};
}

template <typename T>
const std::shared_ptr<const pmr_vector<uint32_t>>& VariableStringDictionarySegment<T>::offset_vector() const {
  return _offset_vector;
}

template class VariableStringDictionarySegment<pmr_string>;

}  // namespace hyrise
