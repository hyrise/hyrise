#pragma once

#include "types.hpp"


namespace opossum {

template <typename T>
class ColumnValue {
 public:
  ColumnValue(const T & value, const ChunkOffset & chunk_offset) : _value{value}, _chunk_offset{chunk_offset} {}

  const T & value() const { return _value; }
  bool is_null() const { return false; }
  ChunkOffset chunk_offset() const { return _chunk_offset; }

 private:
  const T & _value;
  const ChunkOffset _chunk_offset;
};

template <typename T>
class NullableColumnValue {
 public:
  NullableColumnValue(const T & value, const bool null_value, const ChunkOffset & chunk_offset)
      : _value{value},
        _null_value{null_value},
        _chunk_offset{chunk_offset} {}

  const T & value() const { return _value; }
  bool is_null() const { return _null_value; }
  ChunkOffset chunk_offset() { return _chunk_offset; }

 private:
  const T & _value;
  const bool _null_value;
  const ChunkOffset _chunk_offset;
};

}  // namespace opossum
