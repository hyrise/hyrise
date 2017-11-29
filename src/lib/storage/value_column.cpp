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
const AllTypeVariant ValueColumn<T>::operator[](const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");
  PerformanceWarning("operator[] used");

  // Column supports null values and value is null
  if (is_nullable() && _null_values->at(chunk_offset)) {
    return NULL_VALUE;
  }

  return _values.at(chunk_offset);
}

template <typename T>
bool ValueColumn<T>::is_null(const ChunkOffset chunk_offset) const {
  return is_nullable() && (*_null_values)[chunk_offset];
}

template <typename T>
const T ValueColumn<T>::get(const ChunkOffset chunk_offset) const {
  DebugAssert(chunk_offset != INVALID_CHUNK_OFFSET, "Passed chunk offset must be valid.");

  Assert(!is_nullable() || !(*_null_values).at(chunk_offset), "Can’t return value of column type because it is null.");
  return _values.at(chunk_offset);
}

template <typename T>
void ValueColumn<T>::append(const AllTypeVariant& val) {
  bool is_null = variant_is_null(val);

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
  bool is_null = variant_is_null(val);

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
const pmr_concurrent_vector<std::optional<T>> ValueColumn<T>::materialize_values() const {
  pmr_concurrent_vector<std::optional<T>> values(_values.size(), std::nullopt, _values.get_allocator());

  for (ChunkOffset chunk_offset = 0; chunk_offset < _values.size(); ++chunk_offset) {
    if (is_null(chunk_offset)) continue;
    values[chunk_offset] = _values[chunk_offset];
  }

  return values;
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
void ValueColumn<T>::visit(ColumnVisitable& visitable, std::shared_ptr<ColumnVisitableContext> context) const {
  visitable.handle_value_column(*this, std::move(context));
}

template <typename T>
void ValueColumn<T>::write_string_representation(std::string& row_string, const ChunkOffset chunk_offset) const {
  std::stringstream buffer;

  bool is_null = is_nullable() && (*_null_values)[chunk_offset];
  Assert(!is_null, "This operation does not support NULL values.");

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

template <typename T>
std::shared_ptr<BaseColumn> ValueColumn<T>::copy_using_allocator(const PolymorphicAllocator<size_t>& alloc) const {
  pmr_concurrent_vector<T> new_values(_values, alloc);
  if (is_nullable()) {
    pmr_concurrent_vector<bool> new_null_values(*_null_values, alloc);
    return std::allocate_shared<ValueColumn<T>>(alloc, std::move(new_values), std::move(new_null_values));
  } else {
    return std::allocate_shared<ValueColumn<T>>(alloc, std::move(new_values));
  }
}

EXPLICITLY_INSTANTIATE_DATA_TYPES(ValueColumn);

}  // namespace opossum
