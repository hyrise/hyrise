#include "dictionary_column.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_attribute_vector.hpp"
#include "column_visitable.hpp"
#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"
#include "value_column.hpp"

namespace opossum {

template <typename T>
DictionaryColumn<T>::DictionaryColumn(pmr_vector<T>&& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(std::make_shared<pmr_vector<T>>(std::move(dictionary))), _attribute_vector(attribute_vector) {}

template <typename T>
DictionaryColumn<T>::DictionaryColumn(const std::shared_ptr<pmr_vector<T>>& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(dictionary), _attribute_vector(attribute_vector) {}

template <typename T>
const AllTypeVariant DictionaryColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  PerformanceWarning("operator[] used");

  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(chunk_offset);

  if (value_id == NULL_VALUE_ID) {
    return NULL_VALUE;
  }

  return (*_dictionary)[value_id];
}

template <typename T>
bool DictionaryColumn<T>::is_null(const ChunkOffset chunk_offset) const {
  return _attribute_vector->get(chunk_offset) == NULL_VALUE_ID;
}

template <typename T>
const T DictionaryColumn<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(chunk_offset);

  DebugAssert(value_id != NULL_VALUE_ID, "Value at index " + std::to_string(chunk_offset) + " is null.");

  return (*_dictionary)[value_id];
}

template <typename T>
void DictionaryColumn<T>::append(const AllTypeVariant&) {
  Fail("DictionaryColumn is immutable");
}

template <typename T>
std::shared_ptr<const pmr_vector<T>> DictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const BaseAttributeVector> DictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const pmr_concurrent_vector<std::optional<T>> DictionaryColumn<T>::materialize_values() const {
  pmr_concurrent_vector<std::optional<T>> values(_attribute_vector->size(), std::nullopt, _dictionary->get_allocator());

  for (ChunkOffset chunk_offset = 0; chunk_offset < _attribute_vector->size(); ++chunk_offset) {
    if (is_null(chunk_offset)) continue;
    values[chunk_offset] = (*_dictionary)[_attribute_vector->get(chunk_offset)];
  }

  return values;
}

template <typename T>
const T& DictionaryColumn<T>::value_by_value_id(ValueID value_id) const {
  DebugAssert(value_id != NULL_VALUE_ID, "Null value id passed.");

  return _dictionary->at(value_id);
}

template <typename T>
ValueID DictionaryColumn<T>::lower_bound(T value) const {
  auto it = std::lower_bound(_dictionary->cbegin(), _dictionary->cend(), value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID DictionaryColumn<T>::lower_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  auto typed_value = type_cast<T>(value);
  return static_cast<ValueID>(lower_bound(typed_value));
}

template <typename T>
ValueID DictionaryColumn<T>::upper_bound(T value) const {
  auto it = std::upper_bound(_dictionary->cbegin(), _dictionary->cend(), value);
  if (it == _dictionary->cend()) return INVALID_VALUE_ID;
  return static_cast<ValueID>(std::distance(_dictionary->cbegin(), it));
}

template <typename T>
ValueID DictionaryColumn<T>::upper_bound(const AllTypeVariant& value) const {
  DebugAssert(!variant_is_null(value), "Null value passed.");

  auto typed_value = type_cast<T>(value);
  return static_cast<ValueID>(upper_bound(typed_value));
}

template <typename T>
size_t DictionaryColumn<T>::unique_values_count() const {
  return _dictionary->size();
}

template <typename T>
size_t DictionaryColumn<T>::size() const {
  return _attribute_vector->size();
}

template <typename T>
void DictionaryColumn<T>::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_dictionary_column(*this, std::move(context));
}

template <typename T>
std::shared_ptr<BaseColumn> DictionaryColumn<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  const auto new_attribute_vector = _attribute_vector->copy_using_allocator(alloc);
  const pmr_vector<T> new_dictionary(*_dictionary, alloc);
  return std::allocate_shared<DictionaryColumn<T>>(
      alloc, std::allocate_shared<pmr_vector<T>>(alloc, std::move(new_dictionary)), new_attribute_vector);
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(DictionaryColumn);

}  // namespace opossum
