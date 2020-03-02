#pragma once

#include <algorithm>

#include "storage/run_length_segment.hpp"
#include "storage/segment_iterables.hpp"

#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
class RunLengthSegmentIterable : public PointAccessibleSegmentIterable<RunLengthSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit RunLengthSegmentIterable(const RunLengthSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin =
        Iterator{_segment.values().get(), _segment.null_values().get(), _segment.end_positions().get(), ChunkOffset{0}};
    auto end = Iterator{_segment.values().get(), _segment.null_values().get(), _segment.end_positions().get(),
                        static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    auto begin =
        PointAccessIterator{_segment.values().get(), _segment.null_values().get(), _segment.end_positions().get(),
                            position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator{_segment.values().get(), _segment.null_values().get(),
                                   _segment.end_positions().get(), position_filter->cbegin(), position_filter->cend()};

    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const RunLengthSegment<T>& _segment;

 private:
  class Iterator : public BaseSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = RunLengthSegmentIterable<T>;
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;
    using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;

   public:
    explicit Iterator(const pmr_vector<T>* values, const pmr_vector<bool>* null_values,
                      const pmr_vector<ChunkOffset>* end_positions, ChunkOffset chunk_offset)
        : _values{values},
          _end_positions{end_positions},
          _value_it{values->cbegin()},
          _null_value_it{null_values->cbegin()},
          _end_position_it{end_positions->cbegin()},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;

      if (_chunk_offset > *_end_position_it) {
        ++_value_it;
        ++_null_value_it;
        ++_end_position_it;
      }
    }

    void decrement() {
      --_chunk_offset;

      // Make sure to only check the previous end position when we are not in the very first run.
      if (_end_position_it != _end_positions->cbegin() && _chunk_offset <= *(_end_position_it - 1)) {
        --_value_it;
        --_null_value_it;
        --_end_position_it;
      }
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;

      const auto end_position = std::lower_bound(_end_positions->cbegin(), _end_positions->cend(), _chunk_offset);
      const auto jump_distance_from_begin = std::distance(_end_positions->cbegin(), end_position);
      const auto current_distance_from_begin = std::distance(_end_positions->cbegin(), _end_position_it);
      const auto jump_distance = jump_distance_from_begin - current_distance_from_begin;
      _value_it += jump_distance;
      _null_value_it += jump_distance;
      _end_position_it += jump_distance;
    }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const { return SegmentPosition<T>{*_value_it, *_null_value_it, _chunk_offset}; }

   private:
    const pmr_vector<T>* _values;
    const pmr_vector<ChunkOffset>* _end_positions;

    ValueIterator _value_it;
    NullValueIterator _null_value_it;
    EndPositionIterator _end_position_it;
    ChunkOffset _chunk_offset;
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
  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = RunLengthSegmentIterable<T>;

    explicit PointAccessIterator(const pmr_vector<T>* values, const pmr_vector<bool>* null_values,
                                 const pmr_vector<ChunkOffset>* end_positions,
                                 PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>>{std::move(position_filter_begin),
                                                                                  std::move(position_filter_it)},
          _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          // Estimate threshold using avg run length (i.e., chunk size / run count). The value of 500 has been found by
          // a set of simple TPC-H measurements (see #XXXX). // TODO:
          _linear_search_threshold{static_cast<ChunkOffset>(
              200.0 * std::ceil(static_cast<float>(*(_end_positions->cend() - 1)) / _values->size()))},
          _prev_chunk_offset{0u},
          _prev_index{0ul} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto current_chunk_offset = chunk_offsets.offset_in_referenced_chunk;
      auto end_position_it = _end_positions->cend();

      const int64_t step_size = static_cast<int64_t>(current_chunk_offset) - _prev_chunk_offset;
      /**
       * Depending on the estimated threshold and the step size, a different search approach is used. The threshold
       * estimates how long a jump needs to be before a binary search is faster than linearly searching.
       * Three cases are handled:
       *   - If the chunk offset is smaller then the previous offset (can happen, e.g., after joins), use a binary
       *     search search from the beginning up to the previous position. Whenever reverse iteration is frequently
       *     used, we should consider using linear searching here as well (see below, currently blocked by #1531).
       *   - If the chunk offset is larger than the previous offset and the step size for the next offset is smaller
       *     than the estimated threshold, search linearly from the previous offset up to the end.
       *   - If the chunk offset is larger than the previous offset and the step size for the next offset is larger
       *     than the estimated threshold, use a binary search from the previous offset up to the end.
       */
      if (step_size < 0) {
        end_position_it =
            std::lower_bound(_end_positions->cbegin(), _end_positions->cbegin() + _prev_index, current_chunk_offset);
      } else if (step_size < _linear_search_threshold) {
        const auto less_than_current = [current = current_chunk_offset](ChunkOffset offset) {
          return offset < current;
        };
        end_position_it =
            std::find_if_not(_end_positions->cbegin() + _prev_index, _end_positions->cend(), less_than_current);
      } else {
        end_position_it =
            std::lower_bound(_end_positions->cbegin() + _prev_index, _end_positions->cend(), current_chunk_offset);
      }

      // TODO(anyone): Use std::distance() when #1531 is resolved.
      const auto current_index = end_position_it - _end_positions->cbegin();

      const auto value = (*_values)[current_index];
      const auto is_null = (*_null_values)[current_index];

      _prev_chunk_offset = current_chunk_offset;
      _prev_index = current_index;

      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>* _values;
    const pmr_vector<bool>* _null_values;
    const pmr_vector<ChunkOffset>* _end_positions;

    // Threshold of when to start using a binary search for the next chunk offset instead of a linear search.
    ChunkOffset _linear_search_threshold;

    mutable ChunkOffset _prev_chunk_offset;
    mutable size_t _prev_index;
  };
};

}  // namespace opossum
