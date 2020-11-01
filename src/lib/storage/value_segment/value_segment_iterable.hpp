#pragma once

#include <utility>
#include <vector>

#include "storage/pos_lists/abstract_pos_list.hpp"
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
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    if (_segment.is_nullable()) {
      auto begin = Iterator{&_segment.values(), &_segment.null_values(), 0};
      auto end = Iterator{&_segment.values(), &_segment.null_values(), _segment.size()};
      functor(begin, end);
    } else {
      auto begin = NonNullIterator{&_segment.values(), 0};
      auto end = NonNullIterator{&_segment.values(), _segment.size()};
      functor(begin, end);
    }
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = std::decay_t<decltype(position_filter->cbegin())>;

    if (_segment.is_nullable()) {
      auto begin = PointAccessIterator<PosListIteratorType>{&_segment.values(), &_segment.null_values(),
                                                            position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<PosListIteratorType>{&_segment.values(), &_segment.null_values(),
                                                          position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    } else {
      auto begin = NonNullPointAccessIterator<PosListIteratorType>{&_segment.values(), position_filter->cbegin(),
                                                                   position_filter->cbegin()};
      auto end = NonNullPointAccessIterator<PosListIteratorType>{&_segment.values(), position_filter->cbegin(),
                                                                 position_filter->cend()};
      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const ValueSegment<T>& _segment;

 private:
  class NonNullIterator : public AbstractSegmentIterator<NonNullIterator, NonNullSegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_vector<T>::const_iterator;

   public:
    explicit NonNullIterator(const pmr_vector<T>* values, const ChunkOffset chunk_offset)
        : _values{values},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
    }

    void decrement() {
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
    }

    bool equal(const NonNullIterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const NonNullIterator& other) const { return other._chunk_offset - _chunk_offset; }

    NonNullSegmentPosition<T> dereference() const { return NonNullSegmentPosition<T>{(*_values)[_chunk_offset], _chunk_offset}; }

   private:
    const pmr_vector<T>* _values;
    ChunkOffset _chunk_offset;
  };

  class Iterator : public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = pmr_vector<bool>::const_iterator;

   public:
    explicit Iterator(const pmr_vector<T>* values, const pmr_vector<bool>* null_values, const ChunkOffset chunk_offset)
        : _values{values},
          _null_values{null_values},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
    }

    void decrement() {
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const { return other._chunk_offset - _chunk_offset; }

    SegmentPosition<T> dereference() const { return SegmentPosition<T>{(*_values)[_chunk_offset], (*_null_values)[_chunk_offset], _chunk_offset}; }

   private:
    const pmr_vector<T>* _values;
    const pmr_vector<bool>* _null_values;
    ChunkOffset _chunk_offset;
  };

  template <typename PosListIteratorType>
  class NonNullPointAccessIterator
      : public AbstractPointAccessSegmentIterator<NonNullPointAccessIterator<PosListIteratorType>, NonNullSegmentPosition<T>,
                                                  PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_vector<T>::const_iterator;

   public:
    explicit NonNullPointAccessIterator(const pmr_vector<T>* values, PosListIteratorType position_filter_begin,
                                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<NonNullPointAccessIterator, NonNullSegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _values{values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    NonNullSegmentPosition<T> dereference() const {
      const auto chunk_offsets = this->chunk_offsets();
      return NonNullSegmentPosition<T>{(*_values)[chunk_offsets.offset_in_referenced_chunk],
                                       chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>* _values;
  };

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_vector<T>::const_iterator;
    using NullValueVectorIterator = typename pmr_vector<bool>::const_iterator;

   public:
    explicit PointAccessIterator(const pmr_vector<T>* values, const pmr_vector<bool>* null_values,
                                 PosListIteratorType position_filter_begin, PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _values{values},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto chunk_offsets = this->chunk_offsets();
      return SegmentPosition<T>{(*_values)[chunk_offsets.offset_in_referenced_chunk],
                                (*_null_values)[chunk_offsets.offset_in_referenced_chunk],
                                chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>* _values;
    const pmr_vector<bool>* _null_values;
  };
};

}  // namespace opossum
