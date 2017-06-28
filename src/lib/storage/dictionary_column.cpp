#include "dictionary_column.hpp"

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_attribute_vector.hpp"
#include "utils/assert.hpp"
#include "value_column.hpp"

#include "type_cast.hpp"

namespace opossum {

template <typename T>
DictionaryColumn<T>::DictionaryColumn(const std::vector<T>&& dictionary,
                                      const std::shared_ptr<BaseAttributeVector>& attribute_vector)
    : _dictionary(std::make_shared<std::vector<T>>(std::move(dictionary))), _attribute_vector(attribute_vector) {}

template <typename T>
const AllTypeVariant DictionaryColumn<T>::operator[](const size_t i) const {
  /*
  Handle null values, this is only used for testing the results of joins so far.
  In order to be able to define an expected output table, we need to replace INVALID_CHUNK_OFFSET
  with some printable character, in our case 0, resp. "0".
  Since there is no constructor for String, which takes a numeric 0, we have to differentiate between numbers and
  strings.

  This should be replaced as soon as we have proper NULL values in Opossum.
  Similar code is in value_column.hpp
  */
  if (i == INVALID_CHUNK_OFFSET) {
    if (std::is_same<T, std::string>::value) {
      return "0";
    }
    return T(0);
  }
  return (*_dictionary)[_attribute_vector->get(i)];
}

template <typename T>
const T DictionaryColumn<T>::get(const size_t i) const {
  return (*_dictionary)[_attribute_vector->get(i)];
}

template <typename T>
void DictionaryColumn<T>::append(const AllTypeVariant&) {
  Fail("DictionaryColumn is immutable");
}

template <typename T>
std::shared_ptr<const std::vector<T>> DictionaryColumn<T>::dictionary() const {
  return _dictionary;
}

template <typename T>
std::shared_ptr<const BaseAttributeVector> DictionaryColumn<T>::attribute_vector() const {
  return _attribute_vector;
}

template <typename T>
const tbb::concurrent_vector<T> DictionaryColumn<T>::materialize_values() const {
  tbb::concurrent_vector<T> values(_attribute_vector->size());

  for (ChunkOffset chunk_offset = 0; chunk_offset < _attribute_vector->size(); ++chunk_offset) {
    values[chunk_offset] = (*_dictionary)[_attribute_vector->get(chunk_offset)];
  }

  return values;
}

template <typename T>
const T& DictionaryColumn<T>::value_by_value_id(ValueID value_id) const {
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

  auto value = value_by_value_id(_attribute_vector->get(chunk_offset));
  values_out.push_back(value);
}

template <typename T>
const std::shared_ptr<std::vector<std::pair<RowID, T>>> DictionaryColumn<T>::materialize(
    ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets) {
  auto materialized_vector = std::make_shared<std::vector<std::pair<RowID, T>>>();

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
