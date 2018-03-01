#include "deprecated_dictionary_column.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "column_visitable.hpp"
#include "deprecated_dictionary_column/base_attribute_vector.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "value_column.hpp"

namespace opossum {

template <typename T>
DeprecatedDictionaryColumn<T>::DeprecatedDictionaryColumn(pmr_vector<T>&& dictionary,
                                                          const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(std::make_shared<pmr_vector<T>>(std::move(dictionary))), _attribute_vector(attribute_vector) {}

template <typename T>
DeprecatedDictionaryColumn<T>::DeprecatedDictionaryColumn(const std::shared_ptr<pmr_vector<T>>& dictionary,
                                                          const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(dictionary), _attribute_vector(attribute_vector) {}

template <typename T>
const AllTypeVariant DeprecatedDictionaryColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(chunk_offset);

  if (value_id == NULL_VALUE_ID) {
    return NULL_VALUE;
  }

  return (*_dictionary)[value_id];
}

template <typename T>
bool DeprecatedDictionaryColumn<T>::is_null(const ChunkOffset chunk_offset) const {
  return _attribute_vector->get(chunk_offset) == NULL_VALUE_ID;
}

template <typename T>
const T DeprecatedDictionaryColumn<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(chunk_offset);

  DebugAssert(value_id != NULL_VALUE_ID, "Value at index " + std::to_string(chunk_offset) + " is null.");

  return (*_dictionary)[value_id];
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> DeprecatedDictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const BaseAttributeVector> DeprecatedDictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
ValueID DeprecatedDictionaryColumn<T>::lower_bound(T value) const {
  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID DeprecatedDictionaryColumn<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  auto typed_value = type_cast<T>(value);
  return static_cast<ValueID>(lower_bound(typed_value));
}

template <typename T>
ValueID DeprecatedDictionaryColumn<T>::upper_bound(T value) const {
  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID DeprecatedDictionaryColumn<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  auto typed_value = type_cast<T>(value);
  return static_cast<ValueID>(upper_bound(typed_value));
}

template <typename T>
size_t DeprecatedDictionaryColumn<T>::unique_values_count() const {
  return _dictionary->size();
}

template <typename T>
size_t DeprecatedDictionaryColumn<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
void DeprecatedDictionaryColumn<T>::visit(ColumnVisitable& visitable,
                                          std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_column(*this, std::move(context));
}

template <typename T>
std::shared_ptr<BaseColumn> DeprecatedDictionaryColumn<T>::copy_using_allocator(
    const PolymorphicAllocator<size_t>& alloc) const {
  const auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  const pmr_vector<T> new_dictionary(*_dictionary, alloc);
  return std::allocate_shared<DeprecatedDictionaryColumn<T>>(
      alloc, std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_dictionary)), new_attribute_vector);
}

template <typename T>
size_t DeprecatedDictionaryColumn<T>::estimate_memory_usage() const {
  return sizeof(*this) + _dictionary->size() * sizeof(typename decltype(_dictionary)::element_type::value_type) +
         _attribute_vector->size() * _attribute_vector->width();
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DeprecatedDictionaryColumn);

}  // namespace opossum
