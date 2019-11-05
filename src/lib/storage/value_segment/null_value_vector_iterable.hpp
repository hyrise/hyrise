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
    _segment.access_statistics().on_iterator_create(null_values.size());
    auto begin = Iterator{null_values.cbegin(), null_values.cbegin(), &_segment};
    auto end = Iterator{null_values.cbegin(), null_values.cend(), &_segment};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    const auto& null_values = _segment.null_values();
    _segment.access_statistics().on_iterator_create_with_pos_list(position_filter->size());
    auto begin = PointAccessIterator{null_values, position_filter->cbegin(), position_filter->cbegin(), &_segment};
    auto end = PointAccessIterator{null_values, position_filter->begin(), position_filter->cend(), &_segment};
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
                      const BaseSegment* segment)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it}, _segment{segment} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_null_value_it; }
    void decrement() { --_null_value_it; }
    void advance(std::ptrdiff_t n) { _null_value_it += n; }
    bool equal(const Iterator& other) const { return _null_value_it == other._null_value_it; }
    std::ptrdiff_t distance_to(const Iterator& other) const { return other._null_value_it - _null_value_it; }

    IsNullSegmentPosition dereference() const {
      _segment->access_statistics().on_iterator_dereference(1);
      return IsNullSegmentPosition{*_null_value_it,
                                   static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
    const BaseSegment* _segment;
  };

  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, IsNullSegmentPosition> {
   public:
    using ValueType = bool;
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit PointAccessIterator(const NullValueVector& null_values,
                                 const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it,
                                 const BaseSegment* segment)
        : BasePointAccessSegmentIterator<PointAccessIterator, IsNullSegmentPosition>{std::move(position_filter_begin),
                                                                                     std::move(position_filter_it)},
          _null_values{null_values},
          _segment{segment} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    IsNullSegmentPosition dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      _segment->access_statistics().on_iterator_dereference_using_pos_list(1);
      return IsNullSegmentPosition{_null_values[chunk_offsets.offset_in_referenced_chunk],
                                   chunk_offsets.offset_in_poslist};
    }

   private:
    const NullValueVector& _null_values;
    const BaseSegment* _segment;
  };
};

}  // namespace opossum
