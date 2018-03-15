#pragma once

#include "resolve_type.hpp"
#include "storage/column_iterables.hpp"
#include "storage/column_iterables/chunk_offset_mapping.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "storage/reference_column.hpp"

namespace opossum {

template <typename T>
class ReferenceColumnIterable : public ColumnIterable<ReferenceColumnIterable<T>> {
 public:
  explicit ReferenceColumnIterable(const ReferenceColumn& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto pos_list = _column.pos_list();

    const auto chunk_id_and_chunk_offsets_list = to_chunk_id_and_chunk_offsets_list(*pos_list);
    if (chunk_id_and_chunk_offsets_list) {
      const auto& [referenced_chunk_id, chunk_offsets_list] = *chunk_id_and_chunk_offsets_list;

      _referenced_with_iterators(referenced_chunk_id, chunk_offsets_list, functor);
      return;
    }

    const auto column_id = _column.referenced_column_id();
    const auto table = _column.referenced_table();

    auto begin = Iterator{column_id, table.get(), pos_list.get(), ChunkOffset{0u}};
    auto end = Iterator{static_cast<ChunkOffset>(pos_list->size())};
    functor(begin, end);
  }

 private:
  const ReferenceColumn& _column;

 private:
  template <typename Functor>
  void _referenced_with_iterators(const ChunkID chunk_id, const ChunkOffsetsList& chunk_offsets_list,
                                  const Functor& functor) const {
    const auto column_id = _column.referenced_column_id();
    const auto table = _column.referenced_table();

    const auto base_column = table->get_chunk(chunk_id)->get_column(column_id);

    resolve_column_type<T>(*base_column, [&functor, &chunk_offsets_list](const auto& referenced_column) {
      using ColumnT = std::decay_t<decltype(referenced_column)>;
      constexpr auto is_reference_column = std::is_same_v<ColumnT, ReferenceColumn>;

      // clang-format off
      if constexpr (!is_reference_column) {
        auto iterable = create_iterable_from_column<T>(referenced_column);
        iterable.with_iterators(&chunk_offsets_list, [&](auto it, auto end) { functor(it, end); });
      } else {
        Fail("Reference column must not reference another reference column.");
      }
      // class-format on
    });
  }

  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    // Begin Iterator
    Iterator(const ColumnID column_id, const Table* table, const PosList* pos_list, const ChunkOffset chunk_offset)
        : _column_id{column_id},
          _table{table},
          _pos_list{pos_list},
          _chunk_offset{chunk_offset} {}

    // End Iterator
    Iterator(const ChunkOffset chunk_offset) : Iterator(ColumnID{0u}, nullptr, nullptr, chunk_offset) {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    ColumnIteratorValue<T> dereference() const {
      const auto row_id = (*_pos_list)[_chunk_offset];  // NOLINT

      if (row_id.is_null()) {
        return {T{}, true, _chunk_offset};
      }

      const auto [chunk_id, chunk_offset] = row_id;

      const auto column = _table->get_chunk(chunk_id)->get_column(_column_id);
      const auto variant_value = (*column)[chunk_offset];

      if (variant_is_null(variant_value)) {
        return {T{}, true, _chunk_offset};
      }

      const auto value = type_cast<T>(variant_value);
      return {value, false, _chunk_offset};
    }

   private:
    const ColumnID _column_id;
    const Table* _table;
    const PosList* _pos_list;

    ChunkOffset _chunk_offset;
  };
};

}  // namespace opossum
