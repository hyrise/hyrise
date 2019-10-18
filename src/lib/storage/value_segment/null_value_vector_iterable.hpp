#pragma once

#include <iterator>
#include <utility>

#include "types.hpp"
#include "storage/base_value_segment.hpp"
#include "storage/segment_iterables.hpp"

namespace opossum {

/**
 * This is an iterable for the null value vector of a value segment.
 * It is used for example in the IS NULL implementation of the table scan.
 */
class NullValueVectorIterable : public PointAccessibleSegmentIterable<NullValueVectorIterable> {
 public:
  using ValueType = bool;

  explicit NullValueVectorIterable(const BaseValueSegment& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    const auto& null_values = _segment.null_values();
    auto begin = Iterator{null_values.cbegin(), null_values.cbegin(), _segment.id()};
    auto end = Iterator{null_values.cbegin(), null_values.cend(), _segment.id()};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    const auto& null_values = _segment.null_values();
    auto begin = PointAccessIterator{null_values, position_filter->cbegin(), position_filter->cbegin(), _segment.id()};
    auto end = PointAccessIterator{null_values, position_filter->begin(), position_filter->cend(), _segment.id()};
    functor(begin, end);
  }

  const BaseValueSegment& segment() const {return _segment; }

 private:
  const BaseValueSegment& _segment;

 private:
  class Iterator : public BaseSegmentIterator<Iterator, IsNullSegmentPosition> {
   public:
    using ValueType = bool;
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it,
                      uint32_t segment_id)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it}, _segment_id{segment_id} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_null_value_it; }
    void decrement() { --_null_value_it; }
    void advance(std::ptrdiff_t n) { _null_value_it += n; }
    bool equal(const Iterator& other) const { return _null_value_it == other._null_value_it; }
    std::ptrdiff_t distance_to(const Iterator& other) const { return other._null_value_it - _null_value_it; }

    IsNullSegmentPosition dereference() const {
      SegmentAccessCounter::instance().increase(_segment_id,
                                                SegmentAccessCounter::IteratorAccess);
      return IsNullSegmentPosition{*_null_value_it,
                                   static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
    uint32_t _segment_id;
  };

  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, IsNullSegmentPosition> {
   public:
    using ValueType = bool;
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit PointAccessIterator(const NullValueVector& null_values,
                                 const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it,
                                 uint32_t segment_id)
        : BasePointAccessSegmentIterator<PointAccessIterator, IsNullSegmentPosition>{std::move(position_filter_begin),
                                                                                     std::move(position_filter_it)},
          _null_values{null_values},
          _segment_id{segment_id} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    IsNullSegmentPosition dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      SegmentAccessCounter::instance().increase(_segment_id,
          SegmentAccessCounter::IteratorPointAccess);
      return IsNullSegmentPosition{_null_values[chunk_offsets.offset_in_referenced_chunk],
                                   chunk_offsets.offset_in_poslist};
    }

   private:
    const NullValueVector& _null_values;
    uint32_t _segment_id;
  };
};

}  // namespace opossum
