#include "value_column.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "utils/assert.hpp"

#include "type_cast.hpp"

namespace opossum {

template <typename T>
ValueColumn<T>::ValueColumn(bool nullable) {
  if (nullable) _null_values = std::make_unique<tbb::concurrent_vector<bool>>();
}

template <typename T>
ValueColumn<T>::ValueColumn(tbb::concurrent_vector<T>&& values) : _values(std::move(values)) {}

template <typename T>
ValueColumn<T>::ValueColumn(tbb::concurrent_vector<T>&& values, tbb::concurrent_vector<bool>&& null_values)
    : _values(std::move(values)),
      _null_values(std::make_unique<tbb::concurrent_vector<bool>>(std::move(null_values))) {}

template <typename T>
const AllTypeVariant ValueColumn<T>::operator[](const size_t i) const {
  // TODO(anyone): Shouldn’t this throw an exception?
  if (i == INVALID_CHUNK_OFFSET) {
    return NullValue{};
  }

  // Columns supports null values and value is null
  if (is_nullable() && _null_values->at(i)) {
    return NullValue{};
  }

  return _values.at(i);
}

template <typename T>
const T ValueColumn<T>::get(const size_t i) const {
  Assert(!is_nullable() || !_null_values->at(i), "Can’t return value of column type because it is null.");
  return _values.at(i);
}

template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  bool is_null = (val == AllTypeVariant{});

  if (is_nullable()) {
    _null_values->push_back(is_null);
    _values.push_back(is_null ? T{} : type_cast<T>(val));
    return;
  }

  Assert(!is_null, "ValueColumns is not nullable but value passed is null.");

  _values.push_back(type_cast<T>(val));
}

template <>
void ValueColumn<std::string>::append(const AllTypeVariant& val) {
  bool is_null = (val == AllTypeVariant{});

  if (is_nullable()) {
    _null_values->push_back(is_null);

    if (is_null) {
      _values.push_back(std::string{});
      return;
    }
  }

  Assert(!is_null, "ValueColumns is not nullable but value passed is null.");

  auto typed_val = type_cast<std::string>(val);
  Assert((typed_val.length() <= std::numeric_limits<StringLength>::max()), "String value is too long to append!");

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
bool ValueColumn<T>::is_nullable() const {
  return _null_values != nullptr;
}

template <typename T>
const tbb::concurrent_vector<bool>& ValueColumn<T>::null_values() const {
  DebugAssert(!is_nullable(), "Chunk does not have mvcc columns");

  return *_null_values;
}

template <typename T>
tbb::concurrent_vector<bool>& ValueColumn<T>::null_values() {
  DebugAssert(!is_nullable(), "Chunk does not have mvcc columns");

  return *_null_values;
}

template <typename T>
size_t ValueColumn<T>::size() const {
  return _values.size();
}

template <typename T>
void ValueColumn<T>::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) {
  visitable.handle_value_column(*this, std::move(context));
}

// TODO(mjendruk): Add edge case handling for null values
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

// TODO(mjendruk): Figure out where it is used and decide appropriatedly
template <typename T>
void ValueColumn<T>::copy_value_to_value_column(BaseColumn& value_column, ChunkOffset chunk_offset) const {
  auto& output_column = static_cast<ValueColumn<T>&>(value_column);
  auto& values_out = output_column.values();

  values_out.push_back(_values[chunk_offset]);
}

// TODO(mjendruk): Figure out where it is used and decide appropriatedly
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
