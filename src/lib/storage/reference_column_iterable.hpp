#pragma once

#include "tbb/concurrent_vector.h"

#include <iterator>
#include <map>

#include "value_column.hpp"


namespace opossum {


template <typename T>
class ReferenceColumnIterable
{
 public:
  class NullableColumnValue {
   public:
    NullableColumnValue(const T & value, const bool null_value)
        : _value{value},
          _null_value{null_value} {}

    const T & value() const { return _value; }
    bool is_null() const { return _null_value; }

   private:
    const T & _value;
    const bool _null_value;
  };

  class Iterator : public std::iterator<std::input_iterator_tag, NullableColumnValue, std::ptrdiff_t, NullableColumnValue *, NullableColumnValue> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id, const PosListIterator & pos_list_it)
        : _table{table},
          _column_id{column_id},
          _pos_list_it{pos_list_it} {}

    Iterator& operator++() { ++_pos_list_it; return *this;}
    Iterator operator++(int) { auto retval = *this; ++(*this); return retval; }
    bool operator==(Iterator other) const { return _pos_list_it == other._pos_list_it; }
    bool operator!=(Iterator other) const { return !(*this == other); }

    auto operator*() const {
      if (*_pos_list_it == NULL_ROW_ID)
        return NullableColumnValue{T{}, true};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto chunk_offset = _pos_list_it->chunk_offset;

      auto value_column_it = _value_columns.find(chunk_id);
      if (value_column_it != _value_columns.end()) {
        return value_from_value_column(*(value_column_it->second), chunk_offset);
      }

      auto dict_column_it = _dictionary_columns.find(chunk_id);
      if (dict_column_it != _dictionary_columns.end()) {
        return value_from_dictionary_column(*(dict_column_it->second), chunk_offset);
      }

      const auto & chunk = _table->get_chunk(chunk_id);
      const auto column = chunk.get_column(_column_id);

      if (auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column)) {
        _value_columns[chunk_id] = value_column.get();
        return value_from_value_column(*value_column, chunk_offset);
      }

      if (auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column)) {
        _dictionary_columns[chunk_id] = dict_column.get();
        return value_from_dictionary_column(*dict_column, chunk_offset);
      }

      Fail("Referenced column is neither value nor dictionary column.");
      return NullableColumnValue{T{}, false};
    }

    NullableColumnValue value_from_value_column(const ValueColumn<T> & column, const ChunkOffset & chunk_offset) const {
      if (column.is_nullable()) {
        auto is_null = column.null_values()[chunk_offset];
        const auto & value = is_null ? T{} : column.values()[chunk_offset];
        return NullableColumnValue{value, is_null};
      }

      const auto & value = column.values()[chunk_offset];
      return NullableColumnValue{value, false};
    }

    NullableColumnValue value_from_dictionary_column(const DictionaryColumn<T> & column, const ChunkOffset & chunk_offset) const {
      auto attribute_vector = column.attribute_vector();
      auto value_id = attribute_vector->get(chunk_offset);

      if (value_id == NULL_VALUE_ID) {
        return NullableColumnValue{T{}, true};
      }

      auto dictionary = column.dictionary();
      const auto & value = (*dictionary)[value_id];

      return NullableColumnValue{value, false};
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;

    PosListIterator _pos_list_it;

    mutable std::map<ChunkID, const ValueColumn<T> *> _value_columns;
    mutable std::map<ChunkID, const DictionaryColumn<T> *> _dictionary_columns;
  };

  ReferenceColumnIterable(const ReferenceColumn & column) : _column{column} {}

  template <typename Functor>
  auto execute_for_all(const Functor & func) const {
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();

    auto begin = Iterator{table, column_id, _column.pos_list()->begin()};
    auto end = Iterator{table, column_id, _column.pos_list()->end()};
    return func(begin, end);
  }

 private:
  const ReferenceColumn & _column;
};

}  // namespace opossum
