#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>
#include <type_traits>

#include "storage/column_iterables.hpp"
#include "storage/reference_column.hpp"
#include "resolve_type.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

template <typename T>
class ReferenceColumnIterable : public ColumnIterable<ReferenceColumnIterable<T>> {
 public:
  explicit ReferenceColumnIterable(const ReferenceColumn& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();

    const auto references_single_chunk = (_column.type() == ReferenceColumnType::SingleChunk);
    if (references_single_chunk) {
      const auto chunk_id = _column.pos_list()->front().chunk_id;
      const auto chunk_offsets_list = to_chunk_offsets_list(*_column.pos_list());

      const auto base_column = table->get_chunk(chunk_id)->get_column(column_id);
      resolve_column_type<T>(*base_column, [&](auto& column) {
        using ColumnTypeT = std::decay_t<decltype(column)>;
        constexpr auto is_reference_column = std::is_same_v<ColumnTypeT, ReferenceColumn>;

        if constexpr (!is_reference_column) {
          auto iterable = create_iterable_from_column<T>(column);
          iterable.with_iterators(&chunk_offsets_list, [&](auto it, auto end) { functor(it, end); });
        } else {
          Fail("Reference column must not reference another reference column.");
        }
      });
    } else {  // references multiple chunks
      const auto begin_it = _column.pos_list()->begin();
      const auto end_it = _column.pos_list()->end();

      auto begin = Iterator::create_begin(table, column_id, begin_it, begin_it);
      auto end = Iterator::create_end(end_it);
      functor(begin, end);
    }
  }

 private:
  const ReferenceColumn& _column;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    static Iterator create_begin(const std::shared_ptr<const Table> table, const ColumnID column_id,
                                 const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it) {
      return Iterator{table, column_id, begin_pos_list_it, pos_list_it};
    }
    static Iterator create_end(const PosListIterator& pos_list_it) {
      return Iterator{{}, {}, {}, pos_list_it};
    }

   public:
    explicit Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id,
                      const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it)
        : _table{table}, _column_id{column_id}, _begin_pos_list_it{begin_pos_list_it}, _pos_list_it{pos_list_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator& other) const { return _pos_list_it == other._pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    ColumnIteratorValue<T> dereference() const {
      if (*_pos_list_it == NULL_ROW_ID) return ColumnIteratorValue<T>{T{}, true, 0u};

      const auto& [chunk_id, chunk_offset] = *_pos_list_it;

      auto value_column_it = _value_columns.find(chunk_id);
      if (value_column_it != _value_columns.end()) {
        return _value_from_value_column(*(value_column_it->second), chunk_offset);
      }

      auto dict_column_it = _dictionary_columns.find(chunk_id);
      if (dict_column_it != _dictionary_columns.end()) {
        return _value_from_dictionary_column(*(dict_column_it->second), chunk_offset);
      }

      const auto chunk = _table->get_chunk(chunk_id);
      const auto column = chunk->get_column(_column_id);

      if (auto value_column = std::dynamic_pointer_cast<const ValueColumn<T>>(column)) {
        _value_columns[chunk_id] = value_column;
        return _value_from_value_column(*value_column, chunk_offset);
      }

      if (auto dict_column = std::dynamic_pointer_cast<const DeprecatedDictionaryColumn<T>>(column)) {
        _dictionary_columns[chunk_id] = dict_column;
        return _value_from_dictionary_column(*dict_column, chunk_offset);
      }

      /**
       * This is just a temporary solution for supporting encoded column types.
       * It’s very slow and is going to be replaced very soon!
       */
      return _value_from_any_column(*column, chunk_offset);
    }

   private:
    auto _value_from_value_column(const ValueColumn<T>& column, const ChunkOffset& chunk_offset) const {
      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (column.is_nullable()) {
        auto is_null = column.null_values()[chunk_offset];
        const auto& value = is_null ? T{} : column.values()[chunk_offset];
        return ColumnIteratorValue<T>{value, is_null, chunk_offset_into_ref_column};
      }

      const auto& value = column.values()[chunk_offset];
      return ColumnIteratorValue<T>{value, false, chunk_offset_into_ref_column};
    }

    auto _value_from_dictionary_column(const DeprecatedDictionaryColumn<T>& column,
                                       const ChunkOffset& chunk_offset) const {
      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));
      auto attribute_vector = column.attribute_vector();
      auto value_id = attribute_vector->get(chunk_offset);

      if (value_id == NULL_VALUE_ID) {
        return ColumnIteratorValue<T>{T{}, true, chunk_offset_into_ref_column};
      }

      auto dictionary = column.dictionary();
      const auto& value = (*dictionary)[value_id];

      return ColumnIteratorValue<T>{value, false, chunk_offset_into_ref_column};
    }

    auto _value_from_any_column(const BaseColumn& column, const ChunkOffset& chunk_offset) const {
      const auto variant_value = column[chunk_offset];

      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (variant_is_null(variant_value)) {
        return ColumnIteratorValue<T>{T{}, true, chunk_offset_into_ref_column};
      }

      return ColumnIteratorValue<T>{type_cast<T>(variant_value), false, chunk_offset_into_ref_column};
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    mutable std::map<ChunkID, std::shared_ptr<const ValueColumn<T>>> _value_columns;
    mutable std::map<ChunkID, std::shared_ptr<const DeprecatedDictionaryColumn<T>>> _dictionary_columns;
  };
};

}  // namespace opossum
