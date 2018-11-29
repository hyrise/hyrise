#pragma once

#include <algorithm>

#include "storage/segment_iterables.hpp"

#include "storage/run_length_segment.hpp"

namespace opossum {

template <typename T>
class RunLengthSegmentIterable : public PointAccessibleSegmentIterable<RunLengthSegmentIterable<T>> {
 public:
  using ColumnDataType = T;

  explicit RunLengthSegmentIterable(const RunLengthSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin =
        Iterator{_segment.values()->cbegin(), _segment.null_values()->cbegin(), _segment.end_positions()->cbegin(), 0u};
    auto end = Iterator{_segment.values()->cend(), _segment.null_values()->cend(), _segment.end_positions()->cend(),
                        static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    auto begin = PointAccessIterator{*_segment.values(), *_segment.null_values(), *_segment.end_positions(),
                                     position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator{*_segment.values(), *_segment.null_values(), *_segment.end_positions(),
                                   position_filter->cbegin(), position_filter->cend()};

    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const RunLengthSegment<T>& _segment;

 private:
  class Iterator : public BaseSegmentIterator<Iterator, SegmentIteratorValue<T>> {
   public:
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;
    using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;

   public:
    explicit Iterator(const ValueIterator& value_it, const NullValueIterator& null_value_it,
                      const EndPositionIterator& end_position_it, const ChunkOffset start_position)
        : _value_it{value_it},
          _null_value_it{null_value_it},
          _end_position_it{end_position_it},
          _current_position{start_position} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_current_position;

      if (_current_position > *_end_position_it) {
        ++_value_it;
        ++_null_value_it;
        ++_end_position_it;
      }
    }

    bool equal(const Iterator& other) const { return _current_position == other._current_position; }

    SegmentIteratorValue<T> dereference() const {
      return SegmentIteratorValue<T>{*_value_it, *_null_value_it, _current_position};
    }

   private:
    ValueIterator _value_it;
    NullValueIterator _null_value_it;
    EndPositionIterator _end_position_it;
    ChunkOffset _current_position;
  };

  /**
   * Due to the nature of the encoding, point-access is not in O(1).
   * However, because we store the last position of runs (i.e. a sorted list)
   * instead of the run length, it is possible to find the value of a position
   * in O(log(n)) by doing a binary search. Because of the prefetching
   * capabilities of the hardware, this might not always be faster than a simple
   * linear search in O(n). More often than not, the chunk offsets will be ordered,
   * so we don’t even have to scan the entire vector. Instead we can continue searching
   * from the previously requested position. This is what this iterator does:
   * - if it’s the first access, it performs a binary search
   * - for all subsequent accesses it performs
   *   - a linear search in the range [previous_end_position, n] if new_pos >= previous_pos
   *   - a binary search in the range [0, previous_end_position] else
   */
  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, SegmentIteratorValue<T>> {
   public:
    explicit PointAccessIterator(const pmr_vector<T>& values, const pmr_vector<bool>& null_values,
                                 const pmr_vector<ChunkOffset>& end_positions,
                                 const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentIteratorValue<T>>{std::move(position_filter_begin),
                                                                                       std::move(position_filter_it)},
          _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _prev_chunk_offset{end_positions.back() + 1u},
          _prev_index{end_positions.size()} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto current_chunk_offset = chunk_offsets.offset_in_referenced_chunk;
      const auto less_than_current = [current = current_chunk_offset](ChunkOffset offset) { return offset < current; };

      auto end_position_it = _end_positions.cend();

      if (current_chunk_offset < _prev_chunk_offset) {
        end_position_it =
            std::lower_bound(_end_positions.cbegin(), _end_positions.cbegin() + _prev_index, current_chunk_offset);
      } else {
        end_position_it =
            std::find_if_not(_end_positions.cbegin() + _prev_index, _end_positions.cend(), less_than_current);
      }

      const auto current_index = std::distance(_end_positions.cbegin(), end_position_it);

      const auto value = _values[current_index];
      const auto is_null = _null_values[current_index];

      _prev_chunk_offset = current_chunk_offset;
      _prev_index = current_index;

      return SegmentIteratorValue<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>& _values;
    const pmr_vector<bool>& _null_values;
    const pmr_vector<ChunkOffset>& _end_positions;

    mutable ChunkOffset _prev_chunk_offset;
    mutable size_t _prev_index;
  };
};

}  // namespace opossum
