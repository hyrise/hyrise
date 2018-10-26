#pragma once

#include <utility>
#include <vector>

#include "storage/segment_iterables.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

template <typename T>
class ValueSegmentIterable : public PointAccessibleSegmentIterable<ValueSegmentIterable<T>> {
 public:
  explicit ValueSegmentIterable(const ValueSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    if (_segment.is_nullable()) {
      auto begin = Iterator{_segment.values().cbegin(), _segment.values().cbegin(), _segment.null_values().cbegin()};
      auto end = Iterator{_segment.values().cbegin(), _segment.values().cend(), _segment.null_values().cend()};
      functor(begin, end);
      return;
    }

    auto begin = NonNullIterator{_segment.values().cbegin(), _segment.values().cbegin()};
    auto end = NonNullIterator{_segment.values().cend(), _segment.values().cend()};
    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const PosList& position_filter, const Functor& functor) const {
    if (_segment.is_nullable()) {
      auto begin = PointAccessIterator{_segment.values(), _segment.null_values(), position_filter.cbegin(),
                                       position_filter.cbegin()};
      auto end = PointAccessIterator{_segment.values(), _segment.null_values(), position_filter.cbegin(),
                                     position_filter.cend()};
      functor(begin, end);
    } else {
      auto begin = NonNullPointAccessIterator{_segment.values(), position_filter.cbegin(), position_filter.cbegin()};
      auto end = NonNullPointAccessIterator{_segment.values(), position_filter.cbegin(), position_filter.cend()};
      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ValueSegment<T>& _segment;

 private:
  class NonNullIterator : public BaseSegmentIterator<NonNullIterator, NonNullSegmentIteratorValue<T>> {
   public:
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
    bool equal(const NonNullIterator& other) const { return _value_it == other._value_it; }

    NonNullSegmentIteratorValue<T> dereference() const {
      return NonNullSegmentIteratorValue<T>{*_value_it, _chunk_offset};
    }

   private:
    ValueIterator _value_it;
    ChunkOffset _chunk_offset;
  };

  class Iterator : public BaseSegmentIterator<Iterator, SegmentIteratorValue<T>> {
   public:
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

    bool equal(const Iterator& other) const { return _value_it == other._value_it; }

    SegmentIteratorValue<T> dereference() const {
      return SegmentIteratorValue<T>{*_value_it, *_null_value_it, _chunk_offset};
    }

   private:
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
    ChunkOffset _chunk_offset;
  };

  class NonNullPointAccessIterator
      : public BasePointAccessSegmentIterator<NonNullPointAccessIterator, SegmentIteratorValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;

   public:
    explicit NonNullPointAccessIterator(const ValueVector& values, const PosList::const_iterator position_filter_begin,
                                        PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<NonNullPointAccessIterator,
                                         SegmentIteratorValue<T>>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _values{values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      return SegmentIteratorValue<T>{_values[chunk_offsets.offset_in_referenced_chunk], false,
                                     chunk_offsets.offset_in_poslist};
    }

   private:
    const ValueVector& _values;
  };

  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, SegmentIteratorValue<T>> {
   public:
    using ValueVector = pmr_concurrent_vector<T>;
    using NullValueVector = pmr_concurrent_vector<bool>;

   public:
    explicit PointAccessIterator(const ValueVector& values, const NullValueVector& null_values,
                                 const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentIteratorValue<T>>{std::move(position_filter_begin),
                                                                                       std::move(position_filter_it)},
          _values{values},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      return SegmentIteratorValue<T>{_values[chunk_offsets.offset_in_referenced_chunk],
                                     _null_values[chunk_offsets.offset_in_referenced_chunk],
                                     chunk_offsets.offset_in_poslist};
    }

   private:
    const ValueVector& _values;
    const NullValueVector& _null_values;
  };
};

}  // namespace opossum
