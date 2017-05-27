#include "value_column.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "type_cast.hpp"

namespace opossum {

template <typename T>
ValueColumn<T>::ValueColumn(tbb::concurrent_vector<T>&& values) : _values(std::move(values)) {}

template <typename T>
const AllTypeVariant ValueColumn<T>::operator[](const size_t i) const {
  /*
  Handle null values, this is only used for testing the results of joins so far.
  In order to be able to define an expected output table, we need to replace INVALID_CHUNK_OFFSET
  with some printable character, in our case 0, resp. "0".
  Since there is no constructor for String, which takes a numeric 0, we have to differentiate between numbers and
  strings.

  This should be replaced as soon as we have proper NULL values in Opossum.
  Similar code is in dictionary_column.hpp
  */
  if (i == INVALID_CHUNK_OFFSET) {
    if (std::is_same<T, std::string>::value) {
      return "0";
    }
    return T(0);
  }
  return _values.at(i);
}

template <typename T>
const T ValueColumn<T>::get(const size_t i) const {
  return _values.at(i);
}

template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  _values.push_back(type_cast<T>(val));
}

template <>
void ValueColumn<std::string>::append(const AllTypeVariant& val) {
  auto typed_val = type_cast<std::string>(val);
  if (typed_val.length() > std::numeric_limits<StringLength>::max()) {
    throw std::runtime_error("String value is too long to append!");
  }
  _values.push_back(typed_val);
}

template <typename T>
const tbb::concurrent_vector<T>& ValueColumn<T>::values() const {
  return _values;
}

template <typename T>
tbb::concurrent_vector<T>& ValueColumn<T>::values() {
  return _values;
}

template <typename T>
size_t ValueColumn<T>::size() const {
  return _values.size();
}

template <typename T>
void ValueColumn<T>::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) {
  visitable.handle_value_column(*this, std::move(context));
}

template <typename T>
void ValueColumn<T>::write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const {
  std::stringstream buffer;
  // buffering value at chunk_offset
  buffer << _values[chunk_offset];
  uint32_t length = buffer.str().length();
  // writing byte representation of length
  buffer.write(reinterpret_cast<const char*>(&length), sizeof(length));

  // appending the new string to the already present string
  row_string += buffer.str();
}

template <typename T>
void ValueColumn<T>::copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const {
  auto& output_column = static_cast<ValueColumn<T>&>(value_column);
  auto& values_out = output_column.values();

  values_out.push_back(_values[chunk_offset]);
}

template <typename T>
const std::shared_ptr<std::vector<std::pair<RowID, T>>> ValueColumn<T>::materialize(
    ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets) {
  auto materialized_vector = std::make_shared<std::vector<std::pair<RowID, T>>>();

  // we may want to sort offsets first?
  if (offsets) {
    materialized_vector->reserve(offsets->size());
    for (auto& offset : *offsets) {
      auto materialized_row = std::make_pair(RowID{chunk_id, offset}, _values[offset]);
      materialized_vector->push_back(materialized_row);
    }
  } else {
    materialized_vector->reserve(_values.size());
    for (ChunkOffset offset = 0; offset < _values.size(); offset++) {
      auto materialized_row = std::make_pair(RowID{chunk_id, offset}, _values[offset]);
      materialized_vector->push_back(materialized_row);
    }
  }

  return materialized_vector;
}

template class ValueColumn<int32_t>;
template class ValueColumn<int64_t>;
template class ValueColumn<float>;
template class ValueColumn<double>;
template class ValueColumn<std::string>;

}  // namespace opossum
