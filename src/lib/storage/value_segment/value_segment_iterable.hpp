#pragma once

#include <utility>
#include <vector>

#include "storage/buffer/pin_guard.hpp"
#include "storage/pos_lists/abstract_pos_list.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/value_segment.hpp"

namespace hyrise {

template <typename T>
class ValueSegmentIterable : public PointAccessibleSegmentIterable<ValueSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit ValueSegmentIterable(const ValueSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    if (_segment.is_nullable()) {
      const auto values = _segment.values();
      const auto null_values = _segment.null_values();
      auto value_pin_guard = FramePinGuard{};
      auto null_value_pin_guard = FramePinGuard{};

      auto begin =
          Iterator{values.cbegin().get_ptr().pin(value_pin_guard), values.cbegin().get_ptr().pin(value_pin_guard),
                   null_values.cbegin().get_ptr().pin(null_value_pin_guard)};
      auto end = Iterator{values.cbegin().get_ptr().pin(value_pin_guard), values.cend().get_ptr().pin(value_pin_guard),
                          null_values.cend().get_ptr().pin(null_value_pin_guard)};
      functor(begin, end);
    } else {
      const auto values = _segment.values();
      auto value_pin_guard = FramePinGuard{};

      auto begin = NonNullIterator{values.cbegin().get_ptr().pin(value_pin_guard),
                                   values.cbegin().get_ptr().pin(value_pin_guard)};
      auto end =
          NonNullIterator{values.cbegin().get_ptr().pin(value_pin_guard), values.cend().get_ptr().pin(value_pin_guard)};
      functor(begin, end);
    }
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = std::decay_t<decltype(position_filter->cbegin())>;

    if (_segment.is_nullable()) {
      auto begin = PointAccessIterator<PosListIteratorType>{_segment.values().cbegin(), _segment.null_values().cbegin(),
                                                            position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<PosListIteratorType>{_segment.values().cbegin(), _segment.null_values().cbegin(),
                                                          position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    } else {
      auto begin = NonNullPointAccessIterator<PosListIteratorType>{
          _segment.values().cbegin(), position_filter->cbegin(), position_filter->cbegin()};
      auto end = NonNullPointAccessIterator<PosListIteratorType>{_segment.values().cbegin(), position_filter->cbegin(),
                                                                 position_filter->cend()};
      functor(begin, end);
    }
  }

  size_t _on_size() const {
    return _segment.size();
  }

 private:
  const ValueSegment<T>& _segment;

 private:
  class NonNullIterator : public AbstractSegmentIterator<NonNullIterator, NonNullSegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using RawValueIterator = typename ValueIterator::pointer::pointer;

   public:
    explicit NonNullIterator(RawValueIterator begin_value_it, RawValueIterator value_it)
        : _value_it(value_it), _chunk_offset{static_cast<ChunkOffset>(std::distance(begin_value_it, value_it))} {}

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

    bool equal(const NonNullIterator& other) const {
      return _value_it == other._value_it;
    }

    std::ptrdiff_t distance_to(const NonNullIterator& other) const {
      return other._value_it - _value_it;
    }

    NonNullSegmentPosition<T> dereference() const {
      return NonNullSegmentPosition<T>{*_value_it, _chunk_offset};
    }

   private:
    RawValueIterator _value_it;
    ChunkOffset _chunk_offset;
  };

  class Iterator : public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = pmr_vector<bool>::const_iterator;

    using RawValueIterator = typename ValueIterator::pointer::pointer;
    using RawNullValueIterator = typename NullValueIterator::pointer::pointer;

   public:
    explicit Iterator(RawValueIterator begin_value_it, RawValueIterator value_it, RawNullValueIterator null_value_it)
        : _value_it(value_it),
          _null_value_it{std::move(null_value_it)},
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

    bool equal(const Iterator& other) const {
      return _value_it == other._value_it;
    }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return other._value_it - _value_it;
    }

    SegmentPosition<T> dereference() const {
      return SegmentPosition<T>{*_value_it, *_null_value_it, _chunk_offset};
    }

   private:
    RawValueIterator _value_it;
    RawNullValueIterator _null_value_it;
    ChunkOffset _chunk_offset;
  };

  template <typename PosListIteratorType>
  class NonNullPointAccessIterator
      : public AbstractPointAccessSegmentIterator<NonNullPointAccessIterator<PosListIteratorType>, SegmentPosition<T>,
                                                  PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_vector<T>::const_iterator;

   public:
    explicit NonNullPointAccessIterator(ValueVectorIterator values_begin_it, PosListIteratorType position_filter_begin,
                                        PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<NonNullPointAccessIterator, SegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _values_begin_it{std::move(values_begin_it)} {}

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

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = ValueSegmentIterable<T>;
    using ValueVectorIterator = typename pmr_vector<T>::const_iterator;
    using NullValueVectorIterator = typename pmr_vector<bool>::const_iterator;

   public:
    explicit PointAccessIterator(ValueVectorIterator values_begin_it, NullValueVectorIterator null_values_begin_it,
                                 PosListIteratorType position_filter_begin, PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _values_begin_it{std::move(values_begin_it)},
          _null_values_begin_it{std::move(null_values_begin_it)} {}

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

}  // namespace hyrise
