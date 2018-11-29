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
  using ColumnDataType = T;

  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto referenced_table = _segment.referenced_table();
    const auto referenced_column_id = _segment.referenced_column_id();

    const auto& pos_list = *_segment.pos_list();

    const auto begin_it = pos_list.begin();
    const auto end_it = pos_list.end();

    // If we are guaranteed that the reference segment refers to a single chunk, we can do some optimizations.
    // For example, we can use a single, non-virtual segment accessor instead of having to keep multiple and using
    // virtual method calls.

    if (pos_list.references_single_chunk() && pos_list.size() > 0) {
      auto referenced_segment = referenced_table->get_chunk(begin_it->chunk_id)->get_segment(referenced_column_id);
      resolve_segment_type<T>(*referenced_segment, [&](const auto& typed_segment) {
        using SegmentType = std::decay_t<decltype(typed_segment)>;

        if constexpr (!std::is_same_v<SegmentType, ReferenceSegment>) {
          auto accessor = SegmentAccessor<T, SegmentType>(typed_segment);

          auto begin = SingleChunkIterator<decltype(accessor)>{accessor, begin_it, begin_it};
          auto end = SingleChunkIterator<decltype(accessor)>{accessor, begin_it, end_it};
          functor(begin, end);
        } else {
          Fail("Found ReferenceSegment pointing to ReferenceSegment");
        }
      });
    } else {
      auto begin = MultipleChunkIterator{referenced_table, referenced_column_id, begin_it, begin_it};
      auto end = MultipleChunkIterator{referenced_table, referenced_column_id, begin_it, end_it};
      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ReferenceSegment& _segment;

 private:
  // The iterator for cases where we iterate over a single referenced chunk
  template <typename Accessor>
  class SingleChunkIterator : public BaseSegmentIterator<SingleChunkIterator<Accessor>, SegmentIteratorValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit SingleChunkIterator(const Accessor& accessor, const PosListIterator& begin_pos_list_it,
                                 const PosListIterator& pos_list_it)
        : _begin_pos_list_it{begin_pos_list_it}, _pos_list_it{pos_list_it}, _accessor{accessor} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const SingleChunkIterator& other) const { return _pos_list_it == other._pos_list_it; }

    SegmentIteratorValue<T> dereference() const {
      if (_pos_list_it->is_null()) return SegmentIteratorValue<T>{T{}, true, 0u};

      const auto& chunk_offset = _pos_list_it->chunk_offset;

      const auto chunk_offset_into_ref_segment =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      const auto typed_value = _accessor.access(chunk_offset);

      if (typed_value) {
        return SegmentIteratorValue<T>{std::move(*typed_value), false, chunk_offset_into_ref_segment};
      } else {
        return SegmentIteratorValue<T>{T{}, true, chunk_offset_into_ref_segment};
      }
    }

   private:
    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    const Accessor _accessor;
  };

  // The iterator for cases where we potentially iterate over multiple referenced chunks
  class MultipleChunkIterator : public BaseSegmentIterator<MultipleChunkIterator, SegmentIteratorValue<T>> {
   public:
    using PosListIterator = PosList::const_iterator;

   public:
    explicit MultipleChunkIterator(const std::shared_ptr<const Table>& referenced_table,
                                   const ColumnID referenced_column_id, const PosListIterator& begin_pos_list_it,
                                   const PosListIterator& pos_list_it)
        : _referenced_table{referenced_table},
          _referenced_column_id{referenced_column_id},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it},
          _accessors{_referenced_table->chunk_count()} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const MultipleChunkIterator& other) const { return _pos_list_it == other._pos_list_it; }

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
      auto segment = _referenced_table->get_chunk(chunk_id)->get_segment(_referenced_column_id);
      auto accessor = std::move(create_segment_accessor<T>(segment));
      _accessors[chunk_id] = std::move(accessor);
    }

   private:
    const std::shared_ptr<const Table> _referenced_table;
    const ColumnID _referenced_column_id;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;

    mutable std::vector<std::shared_ptr<BaseSegmentAccessor<T>>> _accessors;
  };
};

}  // namespace opossum
