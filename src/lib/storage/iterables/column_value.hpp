
#pragma once

#include "types.hpp"

namespace opossum {

/**
 * @defgroup Values returned by iterators from iterables
 *
 * If the underlying column (or data structure) is indexed,
 * i.e. referenced by a reference column, chunk_offset()
 * returns the offset into the reference column. Otherwise
 * it returns an offset into the column.
 *
 * @{
 */

/**
 * This is the most generic column value class
 * and is used in most iterators.
 */
template <typename T>
class NullableColumnValue {
 public:
  NullableColumnValue(const T& value, const bool null_value, const ChunkOffset& chunk_offset)
      : _value{value}, _null_value{null_value}, _chunk_offset{chunk_offset} {}

  const T& value() const { return _value; }
  bool is_null() const { return _null_value; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const T _value;
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

/**
 * Used when the underlying column
 * (or data structure) cannot be null.
 */
template <typename T>
class ColumnValue {
 public:
  ColumnValue(const T& value, const ChunkOffset& chunk_offset) : _value{value}, _chunk_offset{chunk_offset} {}

  const T& value() const { return _value; }
  bool is_null() const { return false; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const T _value;
  const ChunkOffset _chunk_offset;
};

/**
 * Used for data structures that only
 * store if the entry is null or not.
 *
 * @see NullValueVectorIterable
 */
class ColumnNullValue {
 public:
  ColumnNullValue(const bool null_value, const ChunkOffset& chunk_offset)
      : _null_value{null_value}, _chunk_offset{chunk_offset} {}

  bool is_null() const { return _null_value; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

/**@}*/

}  // namespace opossum
