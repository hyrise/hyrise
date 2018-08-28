#pragma once

#include <map>
#include <memory>
#include <utility>
#include <vector>

#include "storage/reference_segment.hpp"
#include "storage/segment_iterables.hpp"

namespace opossum {

template <typename T>
class ReferenceSegmentIterable : public SegmentIterable<ReferenceSegmentIterable<T>> {
 public:
  explicit ReferenceSegmentIterable(const ReferenceSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto table = _segment.referenced_table();
    const auto cxlumn_id = _segment.referenced_cxlumn_id();

    const auto begin_it = _segment.pos_list()->begin();
    const auto end_it = _segment.pos_list()->end();

    auto begin = Iterator{table, cxlumn_id, begin_it, begin_it};
    auto end = Iterator{table, cxlumn_id, begin_it, end_it};
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
    explicit Iterator(const std::shared_ptr<const Table> table, const CxlumnID cxlumn_id,
                      const PosListIterator& begin_pos_list_it, const PosListIterator& pos_list_it)
        : _table{table},
          _cxlumn_id{cxlumn_id},
          _cached_chunk_id{INVALID_CHUNK_ID},
          _cached_segment{nullptr},
          _begin_pos_list_it{begin_pos_list_it},
          _pos_list_it{pos_list_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_pos_list_it; }

    bool equal(const Iterator& other) const { return _pos_list_it == other._pos_list_it; }

    // TODO(anyone): benchmark if using two maps instead doing the dynamic cast every time really is faster.
    SegmentIteratorValue<T> dereference() const {
      if (_pos_list_it->is_null()) return SegmentIteratorValue<T>{T{}, true, 0u};

      const auto chunk_id = _pos_list_it->chunk_id;
      const auto& chunk_offset = _pos_list_it->chunk_offset;

      if (chunk_id != _cached_chunk_id) {
        _cached_chunk_id = chunk_id;
        const auto chunk = _table->get_chunk(chunk_id);
        _cached_segment = chunk->get_segment(_cxlumn_id);
      }

      /**
       * This is just a temporary solution to supporting encoded segment type.
       * It’s very slow and is going to be replaced very soon!
       */
      return _value_from_any_segment(*_cached_segment, chunk_offset);
    }

   private:
    auto _value_from_any_segment(const BaseSegment& segment, const ChunkOffset& chunk_offset) const {
      const auto variant_value = segment[chunk_offset];

      const auto chunk_offset_into_ref_segment =
          static_cast<ChunkOffset>(std::distance(_begin_pos_list_it, _pos_list_it));

      if (variant_is_null(variant_value)) {
        return SegmentIteratorValue<T>{T{}, true, chunk_offset_into_ref_segment};
      }

      return SegmentIteratorValue<T>{type_cast<T>(variant_value), false, chunk_offset_into_ref_segment};
    }

   private:
    const std::shared_ptr<const Table> _table;
    const CxlumnID _cxlumn_id;

    mutable ChunkID _cached_chunk_id;
    mutable std::shared_ptr<const BaseSegment> _cached_segment;

    const PosListIterator _begin_pos_list_it;
    PosListIterator _pos_list_it;
  };
};

}  // namespace opossum
