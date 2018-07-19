#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "resolve_type.hpp"
#include "storage/base_typed_column.hpp"
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
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it},
          _columns{_table->chunk_count(), nullptr} {
      for (auto chunk_id = ChunkID{0}; chunk_id < _table->chunk_count(); ++chunk_id) {
        _insert_referenced_column(chunk_id);
      }
    }

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator& other) const { return _pos_list_it == other._pos_list_it; }

    ColumnIteratorValue<T> dereference() const {
      if (_pos_list_it->is_null()) return ColumnIteratorValue<T>{T{}, true, 0u};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto& chunk_offset = _pos_list_it->chunk_offset;

      const auto chunk_offset_into_ref_column =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      const auto typed_value = _columns[chunk_id]->get_typed_value(chunk_offset);

      return ColumnIteratorValue<T>{typed_value.first, typed_value.second, chunk_offset_into_ref_column};
    }

   private:
    void _insert_referenced_column(const ChunkID chunk_id) {
      if (_columns[chunk_id] == nullptr) {
        auto column = _table->get_chunk(chunk_id)->get_column(_column_id);
        auto base_column_t = std::dynamic_pointer_cast<const BaseTypedColumn<T>>(column);
        DebugAssert(base_column_t, "Cannot cast to BaseColumnT<T>");
        _columns[chunk_id] = base_column_t;
        //        resolve_column_type<T>(*column, [&](auto& typed_column) {
        //          using ColumnType = typename std::decay<decltype(typed_column)>::type;
        //          if constexpr (std::is_same<ColumnType, ReferenceColumn>::value) {
        //            Fail("ReferenceColumn cannot reference another ReferenceColumn");
        //          } else {
        //            _columns[chunk_id] = &typed_column;
        //          }
        //        });
      }
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    std::vector<std::shared_ptr<const BaseTypedColumn<T>>> _columns;
  };
};

}  // namespace opossum
