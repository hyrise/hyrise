#pragma once

#include <iterator>
#include <memory>
#include <utility>

#include "storage/segment_iterables.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * This is an iterable for the null value vector used by, e.g, value, LZ4, or frame of reference segments.
 * It is used for example in the IS NULL implementation of the table scan.
 */
class NullValueVectorIterable : public PointAccessibleSegmentIterable<NullValueVectorIterable> {
 public:
  using ValueType = bool;

  explicit NullValueVectorIterable(const pmr_vector<bool>& null_values) : _null_values{null_values} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{_null_values.cbegin(), _null_values.cbegin()};
    auto end = Iterator{_null_values.cbegin(), _null_values.cend()};
    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    auto begin = PointAccessIterator{_null_values, position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator{_null_values, position_filter->begin(), position_filter->cend()};
    functor(begin, end);
  }

 private:
  const pmr_vector<bool>& _null_values;

 private:
  class Iterator : public AbstractSegmentIterator<Iterator, IsNullSegmentPosition> {
   public:
    using ValueType = bool;
    using NullValueIterator = pmr_vector<bool>::const_iterator;

   public:
    explicit Iterator(const NullValueIterator& begin_null_value_it, const NullValueIterator& null_value_it)
        : _begin_null_value_it{begin_null_value_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_null_value_it;
    }

    void decrement() {
      --_null_value_it;
    }

    void advance(std::ptrdiff_t n) {
      _null_value_it += n;
    }

    bool equal(const Iterator& other) const {
      return _null_value_it == other._null_value_it;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return other._null_value_it - _null_value_it;
    }

    IsNullSegmentPosition dereference() const {
      return IsNullSegmentPosition{*_null_value_it,
                                   static_cast<ChunkOffset>(std::distance(_begin_null_value_it, _null_value_it))};
    }

   private:
    const NullValueIterator _begin_null_value_it;
    NullValueIterator _null_value_it;
  };

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        IsNullSegmentPosition, PosListIteratorType> {
   public:
    using ValueType = bool;
    using NullValueVector = pmr_vector<bool>;

   public:
    explicit PointAccessIterator(const NullValueVector& null_values, const PosListIteratorType position_filter_begin,
                                 PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator, IsNullSegmentPosition,
                                             PosListIteratorType>{std::move(position_filter_begin),
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

}  // namespace hyrise
