#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "iterables.hpp"

#include "storage/reference_column.hpp"

namespace opossum {

template <typename T>
class ReferenceColumnIterable : public Iterable<ReferenceColumnIterable<T>> {
 public:
  explicit ReferenceColumnIterable(const ReferenceColumn &column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor &f) const {
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();

    const auto begin_it = _column.pos_list()->begin();
    const auto end_it = _column.pos_list()->end();

    auto begin = Iterator{table, column_id, begin_it, begin_it};
    auto end = Iterator{table, column_id, begin_it, end_it};
    f(begin, end);
  }

 private:
  const ReferenceColumn &_column;

 private:
  class Iterator : public BaseIterator<Iterator, NullableColumnValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id,
                      const PosListIterator &begin_pos_list_it, const PosListIterator &pos_list_it)
        : _table{table}, _column_id{column_id}, _begin_pos_list_it{begin_pos_list_it}, _pos_list_it{pos_list_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator &other) const { return _pos_list_it == other._pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    NullableColumnValue<T> dereference() const {
      if (*_pos_list_it == NULL_ROW_ID) return NullableColumnValue<T>{T{}, true, 0u};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto &chunk_offset = _pos_list_it->chunk_offset;

      auto value_column_it = _value_columns.find(chunk_id);
      if (value_column_it != _value_columns.end()) {
        return _value_from_value_column(*(value_column_it->second), chunk_offset);
      }

      auto dict_column_it = _dictionary_columns.find(chunk_id);
      if (dict_column_it != _dictionary_columns.end()) {
        return _value_from_dictionary_column(*(dict_column_it->second), chunk_offset);
      }

      const auto &chunk = _table->get_chunk(chunk_id);
      const auto column = chunk.get_column(_column_id);

      if (auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column)) {
        _value_columns[chunk_id] = value_column;
        return _value_from_value_column(*value_column, chunk_offset);
      }

      if (auto dict_column = std::dynamic_pointer_cast<const DictionaryColumn<T>>(column)) {
        _dictionary_columns[chunk_id] = dict_column;
        return _value_from_dictionary_column(*dict_column, chunk_offset);
      }

      Fail("Referenced column is neither value nor dictionary column.");
      return NullableColumnValue<T>{T{}, false, 0u};
    }

   private:
    auto _value_from_value_column(const ValueColumn<T> &column, const ChunkOffset &chunk_offset) const {
      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (column.is_nullable()) {
        auto is_null = column.null_values()[chunk_offset];
        const auto &value = is_null ? T{} : column.values()[chunk_offset];
        return NullableColumnValue<T>{value, is_null, chunk_offset_into_ref_column};
      }

      const auto &value = column.values()[chunk_offset];
      return NullableColumnValue<T>{value, false, chunk_offset_into_ref_column};
    }

    auto _value_from_dictionary_column(const DictionaryColumn<T> &column, const ChunkOffset &chunk_offset) const {
      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));
      auto attribute_vector = column.attribute_vector();
      auto value_id = attribute_vector->get(chunk_offset);

      if (value_id == NULL_VALUE_ID) {
        return NullableColumnValue<T>{T{}, true, chunk_offset_into_ref_column};
      }

      auto dictionary = column.dictionary();
      const auto &value = (*dictionary)[value_id];

      return NullableColumnValue<T>{value, false, chunk_offset_into_ref_column};
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    mutable std::map<ChunkID, std::shared_ptr<const ValueColumn<T>>> _value_columns;
    mutable std::map<ChunkID, std::shared_ptr<const DictionaryColumn<T>>> _dictionary_columns;
  };
};

}  // namespace opossum
