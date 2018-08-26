#pragma once

#include <algorithm>

#include "storage/column_iterables.hpp"

#include "storage/run_length_column.hpp"

namespace opossum {

template <typename T>
class RunLengthColumnIterable : public PointAccessibleColumnIterable<RunLengthColumnIterable<T>> {
 public:
  explicit RunLengthColumnIterable(const RunLengthColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin =
        Iterator{_column.values()->cbegin(), _column.null_values()->cbegin(), _column.end_positions()->cbegin(), 0u};
    auto end = Iterator{_column.values()->cend(), _column.null_values()->cend(), _column.end_positions()->cend(),
                        static_cast<ChunkOffset>(_column.size())};

    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    auto begin = PointAccessIterator{*_column.values(), *_column.null_values(), *_column.end_positions(),
                                     mapped_chunk_offsets.cbegin()};
    auto end = PointAccessIterator{*_column.values(), *_column.null_values(), *_column.end_positions(),
                                   mapped_chunk_offsets.cend()};

    functor(begin, end);
  }

  size_t _on_size() const { return _column.size(); }

 private:
  const RunLengthColumn<T>& _column;

 private:
  class Iterator : public BaseColumnIterator<Iterator, ColumnIteratorValue<T>> {
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

    ColumnIteratorValue<T> dereference() const {
      return ColumnIteratorValue<T>{*_value_it, *_null_value_it, _current_position};
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
  class PointAccessIterator : public BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>> {
   public:
    explicit PointAccessIterator(const pmr_vector<T>& values, const pmr_vector<bool>& null_values,
                                 const pmr_vector<ChunkOffset>& end_positions,
                                 const ChunkOffsetsIterator& chunk_offsets_it)
        : BasePointAccessColumnIterator<PointAccessIterator, ColumnIteratorValue<T>>{chunk_offsets_it},
          _values{values},
          _null_values{null_values},
          _end_positions{end_positions},
          _prev_chunk_offset{end_positions.back() + 1u},
          _prev_index{end_positions.size()} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      const auto current_chunk_offset = chunk_offsets.into_referenced;
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

      return ColumnIteratorValue<T>{value, is_null, chunk_offsets.into_referencing};
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
