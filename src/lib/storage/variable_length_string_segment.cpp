#include "variable_length_string_segment.hpp"
#include <numeric>

#include "resolve_type.hpp"

namespace hyrise {

VariableLengthStringSegment::VariableLengthStringSegment(
    const std::shared_ptr<const pmr_vector<char>>& dictionary,
    const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<pmr_string>()),
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{attribute_vector->create_base_decompressor()},
      _unique_value_count(std::count(dictionary->begin(), dictionary->end(), '\0')),
      _offset_vector{_generate_offset_vector()} {
  // NULL is represented by _offset_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a VariableLengthStringSegment of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(_offset_vector->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

std::shared_ptr<const pmr_vector<char>> VariableLengthStringSegment::dictionary() const {
  return _dictionary;
}

AllTypeVariant VariableLengthStringSegment::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto value = get_typed_value(chunk_offset);
  return value ? value.value() : NULL_VALUE;
}

ChunkOffset VariableLengthStringSegment::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

std::shared_ptr<AbstractSegment> VariableLengthStringSegment::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<char>>(*_dictionary, alloc);
  auto copy = std::make_shared<VariableLengthStringSegment>(std::move(new_dictionary), std::move(new_attribute_vector));
  copy->access_counter = access_counter;
  return copy;
}

size_t VariableLengthStringSegment::memory_usage(const MemoryUsageCalculationMode mode) const {
  return _attribute_vector->data_size() + _dictionary->capacity() + _offset_vector->capacity();
}

std::optional<CompressedVectorType> VariableLengthStringSegment::compressed_vector_type() const {
  return _attribute_vector->type();
}

EncodingType VariableLengthStringSegment::encoding_type() const {
  return EncodingType::VariableStringLengthDictionary;
}

ValueID VariableLengthStringSegment::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
//  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
//      static_cast<uint64_t>(std::ceil(std::log2(_offset_vector->size())));
  const auto typed_value = boost::get<pmr_string>(value);

  auto args = std::vector<ValueID>(_offset_vector->size());
  std::iota(args.begin(), args.end(), 0);
  auto it = std::lower_bound(args.cbegin(), args.cend(), typed_value,
                             [this](const ValueID valueId, const auto to_find) {
                               return typed_value_of_value_id(valueId) < to_find;
                             });
  if (it == args.cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(args.cbegin(), it))};

//  auto iter = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
//  if (iter == _dictionary->cend()) {
//    return INVALID_VALUE_ID;
//  }
//  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), iter))};
}

ValueID VariableLengthStringSegment::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
//  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
//    static_cast<uint64_t>(std::ceil(std::log2(_offset_vector->size())));
  const auto typed_value = boost::get<pmr_string>(value);

  auto args = std::vector<ValueID>(_offset_vector->size());
  std::iota(args.begin(), args.end(), 0);
  auto it = std::upper_bound(args.cbegin(), args.cend(), typed_value,
                             [this](const auto to_find, const ValueID valueID) {
                               return to_find < typed_value_of_value_id(valueID);
                             });
  if (it == args.cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(args.cbegin(), it))};
}

AllTypeVariant VariableLengthStringSegment::value_of_value_id(const ValueID value_id) const {
  return typed_value_of_value_id(value_id);
}

pmr_string VariableLengthStringSegment::typed_value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _offset_vector->size(), "ValueID out of bounds");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  return pmr_string{_dictionary->data() + _offset_vector->operator[](value_id)};
}

ValueID::base_type VariableLengthStringSegment::unique_values_count() const {
  return _unique_value_count;
}

std::shared_ptr<const BaseCompressedVector> VariableLengthStringSegment::attribute_vector() const {
  return _attribute_vector;
}

ValueID VariableLengthStringSegment::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_offset_vector->size())};
}

std::unique_ptr<const pmr_vector<VariableLengthStringSegment::Offset>> VariableLengthStringSegment::_generate_offset_vector() const {
  auto offset_vector = std::make_unique<pmr_vector<Offset>>();
  const auto dictionary_size = _dictionary->size();
  if (dictionary_size == 0) {
    offset_vector->shrink_to_fit();
    return offset_vector;
  }
  offset_vector->reserve(_unique_value_count);
  // First offset is always 0.
  for (auto offset = size_t{0}; offset < dictionary_size; ++offset) {
    offset_vector->push_back(offset);
    while (_dictionary->operator[](offset++) != '\0');
  }
  offset_vector->shrink_to_fit();
  return offset_vector;
}

}  // namespace hyrise
