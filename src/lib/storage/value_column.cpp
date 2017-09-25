#include "value_column.hpp"

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <utility>
#include <vector>

#include "column_visitable.hpp"

#include "type_cast.hpp"
#include "utils/assert.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
ValueColumn<T>::ValueColumn(bool nullable) {
  if (nullable) _null_values = pmr_concurrent_vector<bool>();
}

template <typename T>
ValueColumn<T>::ValueColumn(const PolymorphicAllocator<T>& alloc, bool nullable) : _values(alloc) {
  if (nullable) _null_values = pmr_concurrent_vector<bool>(alloc);
}

template <typename T>
ValueColumn<T>::ValueColumn(pmr_concurrent_vector<T>&& values) : _values(std::move(values)) {}

template <typename T>
ValueColumn<T>::ValueColumn(pmr_concurrent_vector<T>&& values, pmr_concurrent_vector<bool>&& null_values)
    : _values(std::move(values)), _null_values(std::move(null_values)) {}

template <typename T>
const AllTypeVariant ValueColumn<T>::operator[](const size_t i) const {
  DebugAssert(i != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  PerformanceWarning("operator[] used");

  // Columns supports null values and value is null
  if (is_nullable() && (*_null_values).at(i)) {
    return NULL_VALUE;
  }

  return _values.at(i);
}

template <typename T>
const T ValueColumn<T>::get(const size_t i) const {
  DebugAssert(i != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  Assert(!is_nullable() || !(*_null_values).at(i), "Can’t return value of column type because it is null.");
  return _values.at(i);
}

template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  bool is_null = opossum::is_null(val);

  if (is_nullable()) {
    (*_null_values).push_back(is_null);
    _values.push_back(is_null ? T{} : type_cast<T>(val));
    return;
  }

  Assert(!is_null, "ValueColumns is not nullable but value passed is null.");

  _values.push_back(type_cast<T>(val));
}

template <>
void ValueColumn<std::string>::append(const AllTypeVariant& val) {
  bool is_null = opossum::is_null(val);

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
const pmr_concurrent_vector<T>& ValueColumn<T>::values() const {
  return _values;
}

template <typename T>
pmr_concurrent_vector<T>& ValueColumn<T>::values() {
  return _values;
}

template <typename T>
bool ValueColumn<T>::is_nullable() const {
  return static_cast<bool>(_null_values);
}

template <typename T>
const pmr_concurrent_vector<bool>& ValueColumn<T>::null_values() const {
  DebugAssert(is_nullable(), "This ValueColumn does not support null values.");

  return *_null_values;
}

template <typename T>
pmr_concurrent_vector<bool>& ValueColumn<T>::null_values() {
  DebugAssert(is_nullable(), "This ValueColumn does not support null values.");

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

// TODO(anyone): This method is part of an algorithm that hasn't yet been updated to support null values.
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

  if (is_nullable()) {
    bool is_null = (*_null_values)[chunk_offset];

    DebugAssert(!is_null || output_column.is_nullable(), "Target ValueColumn needs to be nullable as well");

    if (output_column.is_nullable()) {
      auto& null_values_out = output_column.null_values();
      null_values_out.push_back(is_null);
    }

    values_out.push_back(is_null ? T{} : _values[chunk_offset]);

  } else {
    values_out.push_back(_values[chunk_offset]);

    if (output_column.is_nullable()) {
      output_column.null_values().push_back(false);
    }
  }
}

// TODO(anyone): This method is part of an algorithm that hasn't yet been updated to support null values.
template <typename T>
const std::shared_ptr<pmr_vector<std::pair<RowID, T>>> ValueColumn<T>::materialize(
    ChunkID chunk_id, std::shared_ptr<std::vector<ChunkOffset>> offsets) {
  auto materialized_vector = std::make_shared<pmr_vector<std::pair<RowID, T>>>(_values.get_allocator());

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
