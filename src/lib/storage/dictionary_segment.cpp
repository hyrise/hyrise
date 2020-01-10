#include "dictionary_segment.hpp"

#include <memory>
#include <string>

#include "resolve_type.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "utils/size_estimation_utils.hpp"

namespace opossum {

template <typename T>
DictionarySegment<T>::DictionarySegment(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                                        const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                                        const ValueID null_value_id)
    : BaseDictionarySegment(data_type_from_type<T>()),
      _dictionary{dictionary},
      _attribute_vector{attribute_vector},
      _null_value_id{null_value_id},
      _decompressor{_attribute_vector->create_base_decompressor()} {
  // NULL is represented by _dictionary.size(). INVALID_VALUE_ID, which is the highest possible number in
  // ValueID::base_type (2^32 - 1), is needed to represent "value not found" in calls to lower_bound/upper_bound.
  // For a DictionarySegment of the max size Chunk::MAX_SIZE, those two values overlap.

  Assert(_dictionary->size() < std::numeric_limits<ValueID::base_type>::max(), "Input segment too big");
  DebugAssert(ValueID{static_cast<uint32_t>(_dictionary->size())} == _null_value_id,
              "Invalid NULL value id");  // NOLINT
}

template <typename T>
AllTypeVariant DictionarySegment<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto typed_value = get_typed_value(chunk_offset);
  if (!typed_value) {
    return NULL_VALUE;
  }
  return *typed_value;
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> DictionarySegment<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
ChunkOffset DictionarySegment<T>::size() const {
  return static_cast<ChunkOffset>(_attribute_vector->size());
}

template <typename T>
std::shared_ptr<BaseSegment> DictionarySegment<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector_ptr = _attribute_vector->copy_using_allocator(alloc);
  auto new_attribute_vector_sptr = std::shared_ptr<const BaseCompressedVector>(std::move(new_attribute_vector_ptr));
  auto new_dictionary = pmr_vector<T>{*_dictionary, alloc};
  auto new_dictionary_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_dictionary));
  return std::allocate_shared<DictionarySegment<T>>(alloc, new_dictionary_ptr, new_attribute_vector_sptr,
                                                    _null_value_id);
}

template <typename T>
size_t DictionarySegment<T>::memory_usage([[maybe_unused]] const MemoryUsageCalculationMode mode) const {
  const auto common_elements_size = sizeof(*this) + _attribute_vector->data_size();

  if constexpr (std::is_same_v<T, pmr_string>) {  // NOLINT
    return common_elements_size + string_vector_memory_usage(*_dictionary, mode);
  }
  return common_elements_size + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type);
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

  const auto typed_value = boost::get<T>(value);

  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T>
ValueID DictionarySegment<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = boost::get<T>(value);

  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return ValueID{static_cast<ValueID::base_type>(std::distance(_dictionary->cbegin(), it))};
}

template <typename T>
AllTypeVariant DictionarySegment<T>::value_of_value_id(const ValueID value_id) const {
  DebugAssert(value_id < _dictionary->size(), "ValueID out of bounds");
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
  return _null_value_id;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionarySegment);

}  // namespace opossum
