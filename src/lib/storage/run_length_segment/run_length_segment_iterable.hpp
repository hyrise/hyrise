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
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    auto begin = Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
                          _segment.end_positions()->cbegin(), ChunkOffset{0}};
    auto end = Iterator{_segment.values(), _segment.null_values(), _segment.end_positions(),
                        _segment.end_positions()->cend(), static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = decltype(position_filter->cbegin());
    auto begin =
        PointAccessIterator<PosListIteratorType>{_segment.values(), _segment.null_values(), _segment.end_positions(),
                                                 position_filter->cbegin(), position_filter->cbegin()};
    auto end =
        PointAccessIterator<PosListIteratorType>{_segment.values(), _segment.null_values(), _segment.end_positions(),
                                                 position_filter->cbegin(), position_filter->cend()};
    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const RunLengthSegment<T>& _segment;

  /**
   * Due to the nature of Run Length encoding, point-access is not in O(1).
   * Run length segments store the end positions of runs in a sorted vector. To access a particular position, this
   * vector has to be searched for the end position of the according run. It is possible to find the value of a
   * position in O(log(n)) by doing a binary search.
   * In case we have already searched for a nearby position (e.g., a previous filter with a low selectivity resulted
   * in an almost contiguous PosList), we can linearly search for the end position. Since position lists are often
   * ordered and modern CPUs are well at prefetching, linear searches are often faster than a complete binary search.
   * determine_linear_search_offset_distance_threshold() estimates the threshold of when to use a linear or a binary
   * search. Given the previous and the current chunk offset, the threshold is used to determine if a linear search is
   * faster for the given number of skipped positions.
   */

  // LINEAR_SEARCH_ELEMENTS_THRESHOLD denotes the distance for that a linear search is faster than using a binary
  // search on a sorted vector. The value of 200 has been found by a set of simple TPC-H measurements (#2038).
  static constexpr auto LINEAR_SEARCH_VECTOR_DISTANCE_THRESHOLD = 200.0f;

  // This method estimates the threshold up to which linear search is faster for a given run-length vector. Since the
  // end position vector is a highly compressed vector of offsets (depending on actual data), the offset distance up to
  // which linear search is beneficial, can be much larger than LINEAR_SEARCH_VECTOR_DISTANCE_THRESHOLD.
  // Example: for a sorted segment of 65k elements with 10 distinct elements, the expected run length is 6.5k. We would
  // use linear searches for offset distances of LINEAR_SEARCH_VECTOR_DISTANCE_THRESHOLD * 6.5. Hence, for sorted
  // vectors with few distinct values (superb cases for run length encoding), binary searches are barely used.
  static ChunkOffset determine_linear_search_offset_distance_threshold(
      const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions) {
    if (end_positions->empty()) {
      return 0;
    }

    const auto chunk_size = end_positions->back();
    const auto run_count = end_positions->size();

    const auto avg_elements_per_run = static_cast<float>(chunk_size) / run_count;
    return static_cast<ChunkOffset>(LINEAR_SEARCH_VECTOR_DISTANCE_THRESHOLD * std::ceil(avg_elements_per_run));
  }

  using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;
  static EndPositionIterator search_end_positions_for_chunk_offset(
      const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions, const ChunkOffset old_chunk_offset,
      const ChunkOffset new_chunk_offset, const size_t previous_end_position_index,
      const size_t linear_search_threshold) {
    const int64_t step_size = static_cast<int64_t>(new_chunk_offset) - old_chunk_offset;

    /**
     * Depending on the estimated threshold and the step size, a different search approach is used. The threshold
     * estimates how large the offset between the previously searched chunk offset and the currently searched chunk
     * offset needs to be before a binary search is faster than linearly searching. Three cases are handled:
     *   - If the chunk offset is smaller than the previous offset (can happen, e.g., after joins), use a binary
     *     search from the beginning up to the previous position. Whenever reverse iteration is frequently used, we
     *     should consider using linear searching here as well (see below, currently blocked by #1531).
     *   - If the chunk offset is larger than the previous offset and the step size for the next offset is smaller
     *     than the estimated threshold, search linearly from the previous offset up to the end.
     *   - If the chunk offset is larger than the previous offset and the step size for the next offset is larger
     *     than the estimated threshold, use a binary search from the previous offset up to the end.
     */
    if (step_size < 0) {
      return std::lower_bound(end_positions->cbegin(), end_positions->cbegin() + previous_end_position_index,
                              new_chunk_offset);
    } else if (step_size < static_cast<int64_t>(linear_search_threshold)) {
      const auto less_than_current = [&](const ChunkOffset offset) { return offset < new_chunk_offset; };
      return std::find_if_not(end_positions->cbegin() + previous_end_position_index, end_positions->cend(),
                              less_than_current);
    } else {
      return std::lower_bound(end_positions->cbegin() + previous_end_position_index, end_positions->cend(),
                              new_chunk_offset);
    }
  }

 private:
  class Iterator : public BaseSegmentIterator<Iterator, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = RunLengthSegmentIterable<T>;
    using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;

   public:
    explicit Iterator(const std::shared_ptr<const pmr_vector<T>>& values,
                      const std::shared_ptr<const pmr_vector<bool>>& null_values,
                      const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions,
                      EndPositionIterator end_positions_it, ChunkOffset chunk_offset)
        : _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _end_positions_it{std::move(end_positions_it)},
          _end_positions_begin_it{_end_positions->cbegin()},
          _linear_search_threshold{determine_linear_search_offset_distance_threshold(_end_positions)},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      if (_chunk_offset > *_end_positions_it) {
        ++_end_positions_it;
      }
    }

    void decrement() {
      DebugAssert(_chunk_offset > 0,
                  "An iterator pointing at the begin of a segment cannot be decremented, see "
                  "https://eel.is/c++draft/iterator.concept.bidir (iterator can be decremented if "
                  "a dereferencable iterator value precedes it).");
      --_chunk_offset;
      if (_end_positions_it != _end_positions_begin_it && _chunk_offset <= *(_end_positions_it - 1)) {
        // Decrease to previous end position if iterators does not point at the beginning and the chunk offset is
        // less than or equal to the previous end position.
        --_end_positions_it;
      }
    }

    void advance(std::ptrdiff_t n) {
      const auto previous_chunk_offset = _chunk_offset;
      _chunk_offset += n;
      _end_positions_it = search_end_positions_for_chunk_offset(
          _end_positions, previous_chunk_offset, _chunk_offset,
          std::distance(_end_positions->cbegin(), _end_positions_it), _linear_search_threshold);
    }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    SegmentPosition<T> dereference() const {
      const auto vector_offset_for_value = std::distance(_end_positions->cbegin(), _end_positions_it);
      return SegmentPosition<T>{(*_values)[vector_offset_for_value], (*_null_values)[vector_offset_for_value],
                                _chunk_offset};
    }

   private:
    std::shared_ptr<const pmr_vector<T>> _values;
    std::shared_ptr<const pmr_vector<bool>> _null_values;
    std::shared_ptr<const pmr_vector<ChunkOffset>> _end_positions;
    EndPositionIterator _end_positions_it;
    EndPositionIterator _end_positions_begin_it;

    // Threshold of when to start using a binary search for the next chunk offset instead of a linear search.
    ChunkOffset _linear_search_threshold;

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
  template <typename PosListIteratorType>
  class PointAccessIterator : public BasePointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                    SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = RunLengthSegmentIterable<T>;

    explicit PointAccessIterator(const std::shared_ptr<const pmr_vector<T>>& values,
                                 const std::shared_ptr<const pmr_vector<bool>>& null_values,
                                 const std::shared_ptr<const pmr_vector<ChunkOffset>>& end_positions,
                                 const PosListIteratorType position_filter_begin,
                                 PosListIteratorType&& position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>,
                                         PosListIteratorType>{std::move(position_filter_begin),
                                                              std::move(position_filter_it)},
          _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _linear_search_threshold{determine_linear_search_offset_distance_threshold(_end_positions)},
          _prev_chunk_offset{0u},
          _prev_index{0ul} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto current_chunk_offset = chunk_offsets.offset_in_referenced_chunk;

      const auto end_positions_it = search_end_positions_for_chunk_offset(
          _end_positions, _prev_chunk_offset, current_chunk_offset, _prev_index, _linear_search_threshold);
      const auto target_distance_from_begin = std::distance(_end_positions->cbegin(), end_positions_it);

      _prev_chunk_offset = current_chunk_offset;
      _prev_index = target_distance_from_begin;

      return SegmentPosition<T>{(*_values)[target_distance_from_begin], (*_null_values)[target_distance_from_begin],
                                chunk_offsets.offset_in_poslist};
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
