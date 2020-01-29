#pragma once

#include <iterator>
#include <utility>

#include "storage/segment_iterables.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This is an iterable for the null value vector of a value segment.
 * It is used for example in the IS NULL implementation of the table scan.
 */
class NullValueVectorIterable : public PointAccessibleSegmentIterable<NullValueVectorIterable> {
 public:
  using ValueType = bool;

  explicit NullValueVectorIterable(const pmr_concurrent_vector<bool>& null_values) : _null_values{null_values} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{_null_values.cbegin(), _null_values.cbegin()};
    auto end = Iterator{_null_values.cbegin(), _null_values.cend()};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    auto begin = PointAccessIterator{_null_values, position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator{_null_values, position_filter->begin(), position_filter->cend()};
    functor(begin, end);
  }

 private:
  const pmr_concurrent_vector<bool>& _null_values;

 private:
  class Iterator : public BaseSegmentIterator<Iterator, IsNullSegmentPosition> {
   public:
    using ValueType = bool;
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_null_value_it; }
    void decrement() { --_null_value_it; }
    void advance(std::ptrdiff_t n) { _null_value_it += n; }
    bool equal(const Iterator& other) const { return _null_value_it == other._null_value_it; }
    std::ptrdiff_t distance_to(const Iterator& other) const { return other._null_value_it - _null_value_it; }

    IsNullSegmentPosition dereference() const {
      return IsNullSegmentPosition{*_null_value_it,
                                   static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  template <typename _SubIteratorType>
  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator<_SubIteratorType>, IsNullSegmentPosition, _SubIteratorType> {
   public:
    using ValueType = bool;
    using NullValueVector = pmr_concurrent_vector<bool>;
    using SubIteratorType = _SubIteratorType;

   public:
    explicit PointAccessIterator(const NullValueVector& null_values,
                                 const SubIteratorType position_filter_begin,
                                 SubIteratorType position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator,
                                        IsNullSegmentPosition, _SubIteratorType>{std::move(position_filter_begin),
                                          std::move(position_filter_it)},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    IsNullSegmentPosition dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      return IsNullSegmentPosition{_null_values[chunk_offsets.offset_in_referenced_chunk],
                                   chunk_offsets.offset_in_poslist};
    }

   private:
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
