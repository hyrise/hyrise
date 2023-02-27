#include "dictionary_segment.hpp"

#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/storage_manager.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace hyrise {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<T>()),
      _dictionary_base_vector{dictionary},
      _dictionary{std::make_shared<std::span<const T>>(_dictionary_base_vector->data(), _dictionary_base_vector->size())},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()} {
  // NULL is represented by _dictionary_base_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a DictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.
  Assert(_dictionary_base_vector->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<const std::span<const T>>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector)
    : BaseDictionarySegment(data_type_from_type<T>()),
      _dictionary_base_vector{},
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _decompressor{_attribute_vector->create_base_decompressor()} {
  // NULL is represented by _dictionary_base_vector.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a DictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.
  Assert(_dictionary->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
}

template <typename T>
DictionarySegment<T>::DictionarySegment(const uint32_t* start_address)
    : BaseDictionarySegment(data_type_from_type<T>()) {
    const auto encoding_type = PersistedSegmentEncodingType{start_address[ENCODING_TYPE_OFFSET_INDEX]};
    const auto dictionary_size = start_address[DICTIONARY_SIZE_OFFSET_INDEX];
    const auto attribute_vector_size = start_address[ATTRIBUTE_VECTOR_OFFSET_INDEX];

    auto* dictionary_address = reinterpret_cast<const T*>(start_address + HEADER_OFFSET_INDEX);
    auto dictionary_span_pointer = std::make_shared<std::span<const T>>(dictionary_address, dictionary_size);

    switch (encoding_type) {
      case PersistedSegmentEncodingType::Unencoded: {
//        auto* const attribute_vector_address =
//          reinterpret_cast<const T*>(start_address + HEADER_OFFSET_INDEX + dictionary_size_bytes);
//        auto attribute_data_span = std::span<T>(attribute_vector_address, attribute_vector_size);
//        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<T>>(attribute_data_span);
//
//        _dictionary_base_vector = dictionary_span_pointer;
//        _attribute_vector = attribute_vector;
//        _decompressor = _attribute_vector->create_base_decompressor();
        break;
      }
      case PersistedSegmentEncodingType::DictionaryEncoding8Bit: {
        //start_address is expressed as uint32_t pointer, therefore have to add dictionary_size
        auto* const attribute_vector_address =
            reinterpret_cast<const uint8_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size);
        auto attribute_data_span = std::span<const uint8_t>(attribute_vector_address, attribute_vector_size);
        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint8_t>>(attribute_data_span);

        _dictionary = dictionary_span_pointer;
        _attribute_vector = attribute_vector;
        _decompressor = _attribute_vector->create_base_decompressor();

        break;
      }
      case PersistedSegmentEncodingType::DictionaryEncoding16Bit: {
        //start_address is expressed as uint32_t pointer, therefore have to add dictionary_size
        auto* const attribute_vector_address =
            reinterpret_cast<const uint16_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size);
        auto attribute_data_span = std::span<const uint16_t>(attribute_vector_address, attribute_vector_size);
        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint16_t>>(attribute_data_span);

        _dictionary = dictionary_span_pointer;
        _attribute_vector = attribute_vector;
        _decompressor = _attribute_vector->create_base_decompressor();

        break;
      }
      case PersistedSegmentEncodingType::DictionaryEncoding32Bit: {
        //start_address is expressed as uint32_t pointer, therefore have to add dictionary_size
        auto* const attribute_vector_address =
            reinterpret_cast<const uint32_t*>(start_address + HEADER_OFFSET_INDEX + dictionary_size);
        auto attribute_data_span = std::span<const uint32_t>(attribute_vector_address, attribute_vector_size);
        auto attribute_vector = std::make_shared<FixedWidthIntegerVector<uint32_t>>(attribute_data_span);

        _dictionary = dictionary_span_pointer;
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
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
std::shared_ptr<const std::span<const T>> DictionarySegment<T>::dictionary() const {
  // We have no idea how the dictionary will be used, so we do not increment the access counters here
  return _dictionary;
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<AbstractSegment> DictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  Assert(_dictionary_base_vector, "Cannot copy based on span-only DictionarySegment.");
  auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = std::make_shared<pmr_vector<T>>(*_dictionary_base_vector, alloc);
  auto copy = std::make_shared<DictionarySegment<T>>(std::move(new_dictionary), std::move(new_attribute_vector));
  copy->access_counter = access_counter;
  return copy;
}

template <typename T>
size_t DictionarySegment<T>::memory_usage(const MemoryUsageCalculationMode mode) const {
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {
    //this cannot be a mmap-based DictionarySegment -> we only allow mapping FixedStringDictionarySegments
    //therefore dictionary_base_vector will always exist
    return common_elements_size + string_vector_memory_usage(*_dictionary_base_vector, mode);
  }
  return common_elements_size +
         _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
}

template <typename T>
std::optional<CompressedVectorType> DictionarySegment<T>::compressed_vector_type() const {
  return _attribute_vector->type();
}

template <typename T>
EncodingType DictionarySegment<T>::encoding_type() const {
  return EncodingType::Dictionary;
}

template <typename T>
ValueID DictionarySegment<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);

  auto iter = std::lower_bound(_dictionary->begin(), _dictionary->end(), typed_value);
  if (iter == _dictionary->end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->begin(), iter))};
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] +=
      static_cast<uint64_t>(std::ceil(std::log2(_dictionary->size())));
  const auto typed_value = boost::get<T>(value);

  auto iter = std::upper_bound(_dictionary->begin(), _dictionary->end(), typed_value);
  if (iter == _dictionary->end()) {
    return INVALID_VALUE_ID;
  }
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->begin(), iter))};
}

template <typename T>
AllTypeVariant DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary->size(), "ValueID out of bounds");
  access_counter[SegmentAccessCounter::AccessType::Dictionary] += 1;
  return (*_dictionary)[value_id];
}

template <typename T>
ValueID::base_type DictionarySegment<T>::unique_values_count() const {
  return static_cast<ValueID::base_type>(_dictionary->size());
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> DictionarySegment<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID DictionarySegment<T>::null_value_id() const {
  return ValueID{static_cast<ValueID::base_type>(_dictionary->size())};
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace hyrise
