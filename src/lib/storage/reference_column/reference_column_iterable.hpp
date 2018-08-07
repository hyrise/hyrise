#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/column_iterables.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

template <typename T>
class ReferenceColumnIterable : public ColumnIterable<ReferenceColumnIterable<T>> {
 public:
  explicit ReferenceColumnIterable(const ReferenceColumn& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();

    const auto begin_it = _column.pos_list()->begin();
    const auto end_it = _column.pos_list()->end();

    auto begin = Iterator{table, column_id, begin_it, begin_it};
    auto end = Iterator{table, column_id, begin_it, end_it};
    functor(begin, end);
  }

  size_t _on_size() const { return _column.size(); }

 private:
  const ReferenceColumn& _column;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id,
                      const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it)
        : _table{table},
          _column_id{column_id},
          _cached_chunk_id{INVALID_CHUNK_ID},
          _cached_column{nullptr},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator& other) const { return _pos_list_it == other._pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    ColumnIteratorValue<T> dereference() const {
      if (_pos_list_it->is_null()) return ColumnIteratorValue<T>{T{}, true, 0u};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto& chunk_offset = _pos_list_it->chunk_offset;

      if (chunk_id != _cached_chunk_id) {
        _cached_chunk_id = chunk_id;
        const auto chunk = _table->get_chunk(chunk_id);
        _cached_column = chunk->get_column(_column_id);
      }

      /**
       * This is just a temporary solution to supporting encoded column type.
       * It’s very slow and is going to be replaced very soon!
       */
      return _value_from_any_column(*_cached_column, chunk_offset);
    }

   private:
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

    mutable ChunkID _cached_chunk_id;
    mutable std::shared_ptr<const BaseColumn> _cached_column;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;
  };
};

}  // namespace opossum
