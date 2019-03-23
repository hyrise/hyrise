#pragma once

#include <utility>
#include <vector>

#include "storage/segment_iterables.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

template <typename T>
class ValueSegmentIterable : public PointAccessibleSegmentIterable<ValueSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit ValueSegmentIterable(const ValueSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    if (_segment.is_nullable()) {
      auto begin = Iterator{_segment.values().cbegin(), _segment.values().cbegin(), _segment.null_values().cbegin()};
      auto end = Iterator{_segment.values().cbegin(), _segment.values().cend(), _segment.null_values().cend()};
      functor(begin, end);
    } else {
      auto begin = NonNullIterator{_segment.values().cbegin(), _segment.values().cbegin()};
      auto end = NonNullIterator{_segment.values().cend(), _segment.values().cend()};
      functor(begin, end);
    }
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    if (_segment.is_nullable()) {
      auto begin = PointAccessIterator{_segment.values().cbegin(), _segment.null_values().cbegin(),
                                       position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator{_segment.values().cbegin(), _segment.null_values().cbegin(),
                                     position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    } else {
      auto begin =
          NonNullPointAccessIterator{_segment.values().cbegin(), position_filter->cbegin(), position_filter->cbegin()};
      auto end =
          NonNullPointAccessIterator{_segment.values().cbegin(), position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ValueSegment<T>& _segment;

 private:
  class NonNullIterator : public BaseSegmentIterator<NonNullIterator, NonNullSegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;

   public:
    explicit NonNullIterator(const ValueIterator begin_value_it, const ValueIterator value_it)
        : _value_it{value_it}, _chunk_offset{static_cast<ChunkOffset>(std::distance(begin_value_it, value_it))} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_value_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_value_it;
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _value_it += n;
      _chunk_offset += n;
    }

    bool equal(const NonNullIterator& other) const { return _value_it == other._value_it; }

    std::ptrdiff_t distance_to(const NonNullIterator& other) const { return other._value_it - _value_it; }

    NonNullSegmentPosition<T> dereference() const { return NonNullSegmentPosition<T>{*_value_it, _chunk_offset}; }

   private:
    ValueIterator _value_it;
    ChunkOffset _chunk_offset;
  };

  class Iterator : public BaseSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_concurrent_vector<T>::const_iterator;
    using NullValueIterator = pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit Iterator(const ValueIterator begin_value_it, const ValueIterator value_it,
                      const NullValueIterator null_value_it)
        : _value_it(value_it),
          _null_value_it{null_value_it},
          _chunk_offset{static_cast<ChunkOffset>(std::distance(begin_value_it, value_it))} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_value_it;
      ++_null_value_it;
      ++_chunk_offset;
    }

    void decrement() {
      --_value_it;
      --_null_value_it;
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _value_it += n;
      _null_value_it += n;
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _value_it == other._value_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._value_it - _value_it; }

    SegmentPosition<T> dereference() const { return SegmentPosition<T>{*_value_it, *_null_value_it, _chunk_offset}; }

   private:
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
    ChunkOffset _chunk_offset;
  };

  class NonNullPointAccessIterator
      : public BasePointAccessSegmentIterator<NonNullPointAccessIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_concurrent_vector<T>::const_iterator;

   public:
    explicit NonNullPointAccessIterator(ValueVectorIterator values_begin_it,
                                        const PosList::const_iterator position_filter_begin,
                                        PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<NonNullPointAccessIterator, SegmentPosition<T>>{std::move(
                                                                                             position_filter_begin),
                                                                                         std::move(position_filter_it)},
          _values_begin_it{values_begin_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      return SegmentPosition<T>{*(_values_begin_it + chunk_offsets.offset_in_referenced_chunk), false,
                                chunk_offsets.offset_in_poslist};
    }

   private:
    ValueVectorIterator _values_begin_it;
  };

  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_concurrent_vector<T>::const_iterator;
    using NullValueVectorIterator = typename pmr_concurrent_vector<bool>::const_iterator;

   public:
    explicit PointAccessIterator(ValueVectorIterator values_begin_it, NullValueVectorIterator null_values_begin_it,
                                 const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>>{std::move(position_filter_begin),
                                                                                  std::move(position_filter_it)},
          _values_begin_it{values_begin_it},
          _null_values_begin_it{null_values_begin_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      return SegmentPosition<T>{*(_values_begin_it + chunk_offsets.offset_in_referenced_chunk),
                                *(_null_values_begin_it + chunk_offsets.offset_in_referenced_chunk),
                                chunk_offsets.offset_in_poslist};
    }

   private:
    ValueVectorIterator _values_begin_it;
    NullValueVectorIterator _null_values_begin_it;
  };
};

}  // namespace opossum
