#include "dictionary_column.hpp"

#include <memory>
#include <string>

#include "storage/column_visitable.hpp"
#include "storage/value_column.hpp"
#include "storage/vector_compression/base_compressed_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
DictionaryColumn<T>::DictionaryColumn(const std::shared_ptr<const pmr_vector<T>>& dictionary,
                                      const std::shared_ptr<const BaseCompressedVector>& attribute_vector,
                                      const ValueID null_value_id)
    : _dictionary{dictionary}, _attribute_vector{attribute_vector}, _null_value_id{null_value_id} {}

template <typename T>
const AllTypeVariant DictionaryColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  auto decoder = _attribute_vector->create_base_decoder();
  const auto value_id = decoder->get(chunk_offset);

  if (value_id == _null_value_id) {
    return NULL_VALUE;
  }

  return (*_dictionary)[value_id];
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> DictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
size_t DictionaryColumn<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
std::shared_ptr<BaseColumn> DictionaryColumn<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  auto new_attribute_vector_ptr = _attribute_vector->copy_using_allocator(alloc);
  auto new_dictionary = pmr_vector<T>{*_dictionary, alloc};
  auto new_dictionary_ptr = std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_dictionary));
  return std::allocate_shared<DictionaryColumn<T>>(alloc, new_dictionary_ptr, new_attribute_vector_ptr, _null_value_id);
}

template <typename T>
size_t DictionaryColumn<T>::estimate_memory_usage() const {
  return sizeof(*this) + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type) +
         _attribute_vector->data_size();
}

template <typename T>
CompressedVectorType DictionaryColumn<T>::compressed_vector_type() const { return _attribute_vector->type(); }

template <typename T>
ValueID DictionaryColumn<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = type_cast<T>(value);

  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID DictionaryColumn<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  const auto typed_value = type_cast<T>(value);

  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), typed_value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
size_t DictionaryColumn<T>::unique_values_count() const {
  return _dictionary->size();
}

template <typename T>
std::shared_ptr<const BaseCompressedVector> DictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const ValueID DictionaryColumn<T>::null_value_id() const {
  return _null_value_id;
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionaryColumn);

}  // namespace opossum
