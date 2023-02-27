#include "fixed_string_dictionary_segment.hpp"

#include <algorithm>
#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "storage_manager.hpp"

namespace hyrise {

template <typename T>
FixedStringDictionarySegment<T>::FixedStringDictionarySegment(
    const std::shared_ptr<const FixedStringVector>& dictionary,
    const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<pmr_string>()),
      _dictionary{dictionary},
      _dictionary_span{std::make_shared<const FixedStringSpan>(*_dictionary)},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()} {}

template <typename T>
FixedStringDictionarySegment<T>::FixedStringDictionarySegment(const uint32_t* start_address)
  : BaseDictionarySegment(data_type_from_type<T>()) {
  const auto encoding_type = PersistedSegmentEncodingType{start_address[ENCODING_TYPE_OFFSET_INDEX]};
  const auto string_length = start_address[STRING_LENGTH_OFFSET_INDEX];
  const auto dictionary_size = start_address[DICTIONARY_SIZE_OFFSET_INDEX ];
  const auto attribute_vector_size = start_address[ATTRIBUTE_VECTOR_OFFSET_INDEX];

  auto* dictionary_address = reinterpret_cast<const char*>(start_address + HEADER_OFFSET_INDEX);
  auto dictionary_span_pointer = std::make_shared<const FixedStringSpan>(dictionary_address, string_length, dictionary_size);

  switch (encoding_type) {
    case PersistedSegmentEncodingType::Unencoded: {
      Fail("Unencoded Segments are not yet implemented for mmap-based storage.");
      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncoding8Bit: {
      //differently from Numeric DictionarySegments dictionary elements aren't necessarily four bytes long
      //as we calculate with uint32_t* start_address, need to add element_index of four bytes to start_address
      //need to calculate dictionary_size_bytes / 4 as adding value
      auto dictionary_size_bytes = (dictionary_size * string_length);
      if (dictionary_size_bytes % 4 != 0) {
        dictionary_size_bytes += 4 - (dictionary_size_bytes %4 );
      }
      auto* const attribute_vector_address =
        reinterpret_cast<const uint8_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size_bytes/4);
      auto attribute_data_span = std::span<const uint8_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(attribute_data_span);

      _dictionary_span = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncoding16Bit: {
      //start_address is expressed as uint32_t pointer, therefore have to add dictionary_size
      auto dictionary_size_bytes = (dictionary_size * string_length);
      if (dictionary_size_bytes % 4 != 0) {
        dictionary_size_bytes += 4 - (dictionary_size_bytes % 4);
      }
      auto* const attribute_vector_address =
        reinterpret_cast<const uint16_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size_bytes/4);
      auto attribute_data_span = std::span<const uint16_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_data_span);

      _dictionary_span = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncoding32Bit: {
      //start_address is expressed as uint32_t pointer, therefore have to add dictionary_size
      auto dictionary_size_bytes = (dictionary_size * string_length);
      if (dictionary_size_bytes % 4 != 0) {
        dictionary_size_bytes += 4 - (dictionary_size_bytes %4 );
      }
      auto* const attribute_vector_address =
        reinterpret_cast<const uint32_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size_bytes/4);
      auto attribute_data_span = std::span<const uint32_t>(attribute_vector_address, attribute_vector_size);
      auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>(attribute_data_span);

      _dictionary_span = dictionary_span_pointer;
      _attribute_vector = attribute_vector;
      _decompressor = _attribute_vector->create_base_decompressor();

      break;
    }
    case PersistedSegmentEncodingType::DictionaryEncodingBitPacking: {
      Fail("Support for span-based BitPackingVectors for DictionarySegments not implemented yet.");
      break;
    }
    default: {
      Fail("Unsupported EncodingType.");
      break;
    }
  }
}

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
  if (value_id == _dictionary_span->size()) {
    return std::nullopt;
  }
  return _dictionary_span->get_string_at(value_id);
}

template <typename T>
std::shared_ptr<const FixedStringVector> FixedStringDictionarySegment<T>::fixed_string_dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const FixedStringSpan> FixedStringDictionarySegment<T>::fixed_string_dictionary_span() const {
  return _dictionary_span;
}

template <typename T>
ChunkOffset FixedStringDictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<AbstractSegment> FixedStringDictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_dictionary = std::make_shared<FixedStringVector>(*_dictionary, alloc);
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);

  auto copy = std::make_shared<FixedStringDictionarySegment<T>>(new_dictionary, std::move(new_attribute_vector));

  copy->access_counter = access_counter;

  return copy;
}

template <typename T>
size_t FixedStringDictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode /*mode*/) const {
  // MemoryUsageCalculationMode ignored as full calculation is efficient.
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

  auto it = std::lower_bound(_dictionary_span->cbegin(), _dictionary_span->cend(), typed_value);
  if (it == _dictionary_span->cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary_span->cbegin(), it))};
}

template <typename T>
ValueID FixedStringDictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = boost::get<pmr_string>(value);

  auto it = std::upper_bound(_dictionary_span->cbegin(), _dictionary_span->cend(), typed_value);
  if (it == _dictionary_span->cend()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary_span->cbegin(), it))};
}

template <typename T>
AllTypeVariant FixedStringDictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary_span->size(), "ValueID out of bounds");
  return _dictionary_span->get_string_at(value_id);
}

template <typename T>
ValueID::base_type FixedStringDictionarySegment<T>::unique_values_count() const {
  return static_cast<ValueID::base_type>(_dictionary_span->size());
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> FixedStringDictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID FixedStringDictionarySegment<T>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_dictionary_span->size())};
}

template class FixedStringDictionarySegment<pmr_string>;

}  // namespace hyrise
