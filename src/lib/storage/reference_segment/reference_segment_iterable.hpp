#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/reference_segment.hpp"
#include "storage/segment_accessor.hpp"
#include "storage/segment_iterables.hpp"

namespace opossum {

template <typename T>
class ReferenceSegmentIterable : public SegmentIterable<ReferenceSegmentIterable<T>> {
 public:
  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto table = _segment.referenced_table();
    const auto column_id = _segment.referenced_column_id();

    const auto begin_it = _segment.pos_list()->begin();
    const auto end_it = _segment.pos_list()->end();

    auto begin = Iterator{table, column_id, begin_it, begin_it};
    auto end = Iterator{table, column_id, begin_it, end_it};
    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ReferenceSegment& _segment;

 private:
  class Iterator : public BaseSegmentIterator<Iterator, SegmentIteratorValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit Iterator(const std::shared_ptr<const Table> table, const ColumnID column_id,
                      const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it)
        : _table{table},
          _column_id{column_id},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it},
          _accessors{_table->chunk_count()} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator& other) const { return _pos_list_it == other._pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    SegmentIteratorValue<T> dereference() const {
      if (_pos_list_it->is_null()) return SegmentIteratorValue<T>{T{}, true, 0u};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto& chunk_offset = _pos_list_it->chunk_offset;

      const auto chunk_offset_into_ref_segment =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (!_accessors[chunk_id]) {
        _create_accessor(chunk_id);
      }
      const auto typed_value = _accessors[chunk_id]->access(chunk_offset);

      if (typed_value) {
        return SegmentIteratorValue<T>{std::move(*typed_value), false, chunk_offset_into_ref_segment};
      } else {
        return SegmentIteratorValue<T>{T{}, true, chunk_offset_into_ref_segment};
      }
    }

    void _create_accessor(const ChunkID chunk_id) const {
      auto segment = _table->get_chunk(chunk_id)->get_segment(_column_id);
      auto accessor = std::move(create_segment_accessor<T>(segment));
      _accessors[chunk_id] = std::move(accessor);
    }

   private:
    const std::shared_ptr<const Table> _table;
    const ColumnID _column_id;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    mutable std::vector<std::shared_ptr<BaseSegmentAccessor<T>>> _accessors;
  };
};

}  // namespace opossum
