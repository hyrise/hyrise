#include "dictionary_column.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_attribute_vector.hpp"
#include "column_visitable.hpp"
#include "value_column.hpp"

#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
DictionaryColumn<T>::DictionaryColumn(const pmr_vector<T>&& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(std::make_shared<pmr_vector<T>>(std::move(dictionary))), _attribute_vector(attribute_vector) {}

template <typename T>
const AllTypeVariant DictionaryColumn<T>::operator[](const size_t i) const {
  PerformanceWarning("operator[] used");

  DebugAssert(i != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(i);

  if (value_id == NULL_VALUE_ID) {
    return NULL_VALUE;
  }

  return (*_dictionary)[value_id];
}

template <typename T>
const T DictionaryColumn<T>::get(const size_t i) const {
  DebugAssert(i != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  const auto value_id = _attribute_vector->get(i);

  DebugAssert(value_id != NULL_VALUE_ID, "Value at index " + to_string(i) + " is null.");

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

// TODO(anyone): This method is part of an algorithm that hasn’t yet been updated to support null values.
template <typename T>
const pmr_concurrent_vector<T> DictionaryColumn<T>::materialize_values() const {
  pmr_concurrent_vector<T> values(_attribute_vector->size(), T(), _dictionary->get_allocator());

  for (ChunkOffset chunk_offset = 0; chunk_offset < _attribute_vector->size(); ++chunk_offset) {
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
  DebugAssert(!is_null(value), "Null value passed.");

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
  DebugAssert(!is_null(value), "Null value passed.");

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
void DictionaryColumn<T>::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) {
  visitable.handle_dictionary_column(*this, std::move(context));
}

// TODO(anyone): This method is part of an algorithm that hasn’t yet been updated to support null values.
template <typename T>
void DictionaryColumn<T>::write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const {
  std::stringstream buffer;
  // buffering value at chunk_offset
  T value = _dictionary->at(_attribute_vector->get(chunk_offset));
  buffer << value;
  uint32_t length = buffer.str().length();
  // writing byte representation of length
  buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

  // appending the new string to the already present string
  row_string += buffer.str();
}

template <typename T>
void DictionaryColumn<T>::copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const {
  auto& output_column = static_cast<ValueColumn<T>&>(value_column);
  auto& values_out = output_column.values();

  auto value_id = _attribute_vector->get(chunk_offset);

  if (output_column.is_nullable()) {
    output_column.null_values().push_back(value_id == NULL_VALUE_ID);
    values_out.push_back(value_id == NULL_VALUE_ID ? T{} : value_by_value_id(value_id));
  } else {
    DebugAssert(value_id != NULL_VALUE_ID, "Target column needs to be nullable");

    values_out.push_back(value_by_value_id(value_id));
  }
}

// TODO(anyone): This method is part of an algorithm that hasn’t yet been updated to support null values.
template <typename T>
const std::shared_ptr<pmr_vector<std::pair<RowID, T>>> DictionaryColumn<T>::materialize(
    ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets) {
  auto materialized_vector = std::make_shared<pmr_vector<std::pair<RowID, T>>>(_dictionary->get_allocator());

  /*
  We only offset if this ValueColumn was referenced by a ReferenceColumn. Thus it might actually be filtered.
  */
  if (offsets) {
    materialized_vector->reserve(offsets->size());
    for (auto& offset : *offsets) {
      T value = (*_dictionary)[_attribute_vector->get(offset)];
      auto materialized_row = std::make_pair(RowID{chunk_id, offset}, value);
      materialized_vector->push_back(materialized_row);
    }
  } else {
    materialized_vector->reserve(_attribute_vector->size());
    for (ChunkOffset offset = 0; offset < _attribute_vector->size(); offset++) {
      T value = (*_dictionary)[_attribute_vector->get(offset)];
      auto materialized_row = std::make_pair(RowID{chunk_id, offset}, value);
      materialized_vector->push_back(materialized_row);
    }
  }

  return materialized_vector;
}

template class DictionaryColumn<int32_t>;
template class DictionaryColumn<int64_t>;
template class DictionaryColumn<float>;
template class DictionaryColumn<double>;
template class DictionaryColumn<std::string>;

}  // namespace opossum
