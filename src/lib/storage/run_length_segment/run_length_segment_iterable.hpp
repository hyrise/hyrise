#pragma once

#include <algorithm>

#include "storage/run_length_segment.hpp"
#include "storage/segment_iterables.hpp"

#include "utils/performance_warning.hpp"

namespace {

using namespace opossum;  // NOLINT

/**
 * Run length segments store the end positions of runs in a sorted vector. To access a particular position, this vector
 * has to be searches for the according run. If we previously visited a nearby position, we do not have to execute a
 * full binary search but can linearly search up to the desired position.
 *
 * determine_linear_search_threshold() estimates the threshold of when to use a linear or a binary search. Given the
 * previous and the current chunk offset, the threshold is used to determine if a linear search is faster for the given
 * number of skipped positions or whether a binary search is beneficial. The value of 200 has been found by a set of
 * simple TPC-H measurements (see #2038).
 */
constexpr auto LINEAR_SEARCH_THRESHOLD_FACTOR = 200.0f;
[[maybe_unused]] ChunkOffset determine_linear_search_threshold(const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions) {
  if (end_positions->empty()) {
    return 0;
  }

  const ChunkOffset chunk_size = end_positions->back();
  const size_t run_count = end_positions->size();

  const auto avg_elements_per_run = static_cast<float>(chunk_size) / run_count;
  return static_cast<ChunkOffset>(LINEAR_SEARCH_THRESHOLD_FACTOR * std::ceil(avg_elements_per_run));
}

using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;
[[maybe_unused]] EndPositionIterator search_end_position_for_chunk_offset(
                                          const ChunkOffset previous_chunk_offset,
                                          const ChunkOffset current_chunk_offset,
                                          const size_t previous_end_position_index,
                                          const size_t linear_search_threshold,
                                          EndPositionIterator end_position_begin_it,
                                          EndPositionIterator end_position_end_it) {
  const int64_t step_size = static_cast<int64_t>(current_chunk_offset) - previous_chunk_offset;
  /**
   * Depending on the estimated threshold and the step size, a different search approach is used. The threshold
   * estimates how long a jump needs to be before a binary search is faster than linearly searching.
   * Three cases are handled:
   *   - If the chunk offset is smaller then the previous offset (can happen, e.g., after joins), use a binary
   *     search from the beginning up to the previous position. Whenever reverse iteration is frequently used, we
   *     should consider using linear searching here as well (see below, currently blocked by #1531).
   *   - If the chunk offset is larger than the previous offset and the step size for the next offset is smaller
   *     than the estimated threshold, search linearly from the previous offset up to the end.
   *   - If the chunk offset is larger than the previous offset and the step size for the next offset is larger
   *     than the estimated threshold, use a binary search from the previous offset up to the end.
   */
  if (step_size < 0) {
    return std::lower_bound(end_position_begin_it, end_position_end_it + previous_end_position_index, current_chunk_offset);
  } else if (step_size < static_cast<int64_t>(linear_search_threshold)) {
    const auto less_than_current = [current = current_chunk_offset](ChunkOffset offset) {
      return offset < current;
    };
    return std::find_if_not(end_position_begin_it + previous_end_position_index, end_position_end_it, less_than_current);
  } else {
    return std::lower_bound(end_position_begin_it + previous_end_position_index, end_position_end_it, current_chunk_offset);
  }
}

} // namespace

namespace opossum {

template <typename T>
class RunLengthSegmentIterable : public PointAccessibleSegmentIterable<RunLengthSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit RunLengthSegmentIterable(const RunLengthSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin =
        Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(), ChunkOffset{0}};
    auto end = Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
                        static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    auto begin = PointAccessIterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
                            position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
                                   position_filter->cbegin(), position_filter->cend()};

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
    explicit Iterator(const std::shared_ptr<const pmr_vector<T>>& values,
                      const std::shared_ptr<const pmr_vector<bool>>& null_values,
                      const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions,
                      ChunkOffset chunk_offset)
        : _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _linear_search_threshold{determine_linear_search_threshold(_end_positions)},
          _chunk_offset{chunk_offset},
          _prev_chunk_offset{0u},
          _prev_index{0ul} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }

    void decrement() { --_chunk_offset; }

    void advance(std::ptrdiff_t n) { _chunk_offset += n; }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    SegmentPosition<T> dereference() const {
      // TODO(anyone): change to std::distance() when #1531 is resolved
      const auto end_position = search_end_position_for_chunk_offset(_prev_chunk_offset, _chunk_offset,
        _prev_index, _linear_search_threshold, _end_positions->cbegin(), _end_positions->cend());
      const auto target_distance_from_begin = end_position - _end_positions->cbegin();

      _prev_chunk_offset = _chunk_offset;
      _prev_index = target_distance_from_begin;

      return SegmentPosition<T>{(*_values)[target_distance_from_begin], (*_null_values)[target_distance_from_begin], _chunk_offset};
    }

   private:
    std::shared_ptr<const pmr_vector<T>> _values;
    std::shared_ptr<const pmr_vector<bool>> _null_values;
    std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;

    // Threshold of when to start using a binary search for the next chunk offset instead of a linear search.
    ChunkOffset _linear_search_threshold;

    ChunkOffset _chunk_offset;

    mutable ChunkOffset _prev_chunk_offset;
    mutable size_t _prev_index;
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
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    explicit PointAccessIterator(const std::shared_ptr<const pmr_vector<T>>& values,
                                 const std::shared_ptr<const pmr_vector<bool>>& null_values,
                                 const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions,
                                 PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>>{std::move(position_filter_begin),
                                                                                  std::move(position_filter_it)},
          _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _linear_search_threshold{determine_linear_search_threshold(_end_positions)},
          _prev_chunk_offset{0u},
          _prev_index{0ul} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto current_chunk_offset = chunk_offsets.offset_in_referenced_chunk;

      // const auto end_position_it = search_end_position_for_chunk_offset(_prev_chunk_offset, current_chunk_offset,
      //   _prev_index, _linear_search_threshold, _end_positions->cbegin(), _end_positions->cend());

      // // TODO(anyone): Use std::distance() when #1531 is resolved.
      // const auto current_index = end_position_it - _end_positions->cbegin();

      // const auto value = *(_values_it + current_index);
      // const auto is_null = *(_null_values_it + current_index);

      // _prev_chunk_offset = current_chunk_offset;
      // _prev_index = current_index;

      const auto end_position = search_end_position_for_chunk_offset(_prev_chunk_offset, current_chunk_offset,
        _prev_index, _linear_search_threshold, _end_positions->cbegin(), _end_positions->cend());
      const auto target_distance_from_begin = end_position - _end_positions->cbegin();

      _prev_chunk_offset = current_chunk_offset;
      _prev_index = target_distance_from_begin;

      return SegmentPosition<T>{(*_values)[target_distance_from_begin], (*_null_values)[target_distance_from_begin], chunk_offsets.offset_in_poslist};
    }

   private:
    std::shared_ptr<const pmr_vector<T>> _values;
    std::shared_ptr<const pmr_vector<bool>> _null_values;
    std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;

    // Threshold of when to start using a binary search for the next chunk offset instead of a linear search.
    ChunkOffset _linear_search_threshold;

    mutable ChunkOffset _prev_chunk_offset;
    mutable size_t _prev_index;
  };
};

}  // namespace opossum
