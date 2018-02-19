#pragma once

#include <map>
#include <memory>
#include <type_traits>
#include <utility>
#include <vector>

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
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();
    const auto& pos_list = *_column.pos_list();

    const auto references_single_chunk = _column.pos_list_type() == PosListType::SingleChunk;
    const auto is_not_empty = !pos_list.empty();

    if (references_single_chunk && is_not_empty) {
      _referenced_with_iterators({pos_list.cbegin(), pos_list.cend(), ChunkOffset{0u}}, functor);
    } else if (!this->single_functor_call_required() && is_not_empty) {
      auto current_chunk_id = pos_list.front().chunk_id;

      auto begin_pos_list_it = pos_list.cbegin();
      auto end_pos_list_it = pos_list.cbegin();
      auto begin_chunk_offset = ChunkOffset{0u};

      while (begin_pos_list_it != pos_list.cend()) {
        ++end_pos_list_it;

        if (end_pos_list_it->chunk_id != current_chunk_id || end_pos_list_it == pos_list.cend()) {
          _referenced_with_iterators({begin_pos_list_it, end_pos_list_it, begin_chunk_offset}, functor);

          begin_chunk_offset += static_cast<ChunkOffset>(std::distance(begin_pos_list_it, end_pos_list_it));
          begin_pos_list_it = end_pos_list_it;
          current_chunk_id = begin_pos_list_it->chunk_id;
        }
      }
    } else {
      auto begin_it = Iterator{table, column_id, pos_list, ChunkOffset{0u}};
      auto end_it = Iterator{table, column_id, pos_list, static_cast<ChunkOffset>(pos_list.size())};

      functor(begin_it, end_it);
    }
  }

 private:
  template <typename Functor>
  void _referenced_with_iterators(const ColumnPointAccessPlan& access_plan, const Functor& functor) const {
    const auto table = _column.referenced_table();
    const auto column_id = _column.referenced_column_id();
    const auto chunk_id = access_plan.begin->chunk_id;
    const auto base_column = table->get_chunk(chunk_id)->get_column(column_id);

    resolve_column_type<T>(*base_column, [&functor, &access_plan](const auto& referenced_column) {
      using ColumnT = std::decay_t<decltype(referenced_column)>;
      constexpr auto is_reference_column = std::is_same_v<ColumnT, ReferenceColumn>;

      // clang-format off
      if constexpr (!is_reference_column) {
        auto iterable = create_iterable_from_column<T>(referenced_column);
        iterable.with_iterators(access_plan, [&](auto it, auto end) { functor(it, end); });
      } else {
        Fail("Reference column must not reference another reference column.");
      }
      // class-format on
    });
  }

  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
   public:
    Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id, const PosList& pos_list,
             ChunkOffset chunk_offset)
        : _table{table},
          _column_id{column_id},
          _pos_list{pos_list},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    ColumnIteratorValue<T> dereference() const {
      const auto [chunk_id, chunk_offset] = _pos_list[_chunk_offset];

      if (chunk_offset == INVALID_CHUNK_OFFSET) {
        return {T{}, true, _chunk_offset};
      }

      const auto column = _table->get_chunk(chunk_id)->get_column(_column_id);
      const auto variant_value = (*column)[chunk_offset];

      if (variant_is_null(variant_value)) {
        return {T{}, true, _chunk_offset};
      }

      const auto value = type_cast<T>(variant_value);
      return {value, false, _chunk_offset};
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;
    const PosList& _pos_list;

    ChunkOffset _chunk_offset;
  };

 private:
  const ReferenceColumn& _column;
};

}  // namespace opossum
