#pragma once

#include <algorithm>

#include "iterables.hpp"

#include "storage/encoded_columns/run_length_column.hpp"

namespace opossum {

template <typename T>
class RunLengthColumnIterable : public IndexableIterable<RunLengthColumnIterable<T>> {
 public:
  explicit RunLengthColumnIterable(const RunLengthColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    auto begin = Iterator{_column.null_value(), _column.values()->cbegin(), _column.end_positions()->cbegin(), 0u};
    auto end = Iterator{_column.null_value(), _column.values()->cend(), _column.end_positions()->cend(),
                        static_cast<ChunkOffset>(_column.size())};

    functor(begin, end);
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    auto begin = IndexedIterator{_column.null_value(), *_column.values(), *_column.end_positions(),
                                 mapped_chunk_offsets.cbegin()};
    auto end =
        IndexedIterator{_column.null_value(), *_column.values(), *_column.end_positions(), mapped_chunk_offsets.cend()};

    functor(begin, end);
  }

 private:
  const RunLengthColumn<T>& _column;

 private:
  class Iterator : public BaseIterator<Iterator, NullableColumnValue<T>> {
   public:
    using ValueIterator = typename pmr_vector<T>::const_iterator;
    using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;

   public:
    explicit Iterator(const T null_value, const ValueIterator& value_it, const EndPositionIterator& end_position_it,
                      const ChunkOffset start_position)
        : _null_value{null_value},
          _value_it{value_it},
          _end_position_it{end_position_it},
          _current_position{start_position} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_current_position;

      if (_current_position > *_end_position_it) {
        ++_value_it;
        ++_end_position_it;
      }
    }

    bool equal(const Iterator& other) const { return _current_position == other._current_position; }

    NullableColumnValue<T> dereference() const {
      if (*_value_it == _null_value) {
        return NullableColumnValue{T{}, true, _current_position};
      }

      return NullableColumnValue<T>{*_value_it, false, _current_position};
    }

   private:
    const T _null_value;
    ValueIterator _value_it;
    EndPositionIterator _end_position_it;
    ChunkOffset _current_position;
  };

  class IndexedIterator : public BaseIndexedIterator<IndexedIterator, NullableColumnValue<T>> {
   public:
    explicit IndexedIterator(const T null_value, const pmr_vector<T>& values,
                             const pmr_vector<ChunkOffset>& end_positions, const ChunkOffsetsIterator& chunk_offsets_it)
        : BaseIndexedIterator<IndexedIterator, NullableColumnValue<T>>{chunk_offsets_it},
          _null_value{null_value},
          _values{values},
          _end_positions{end_positions},
          _prev_chunk_offset{end_positions.back() + 1u},
          _prev_index{end_positions.size()} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    NullableColumnValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return NullableColumnValue<T>{T{}, true, chunk_offsets.into_referencing};

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
      const auto is_null = (value == _null_value);

      _prev_chunk_offset = current_chunk_offset;
      _prev_index = current_index;

      return NullableColumnValue<T>{value, is_null, chunk_offsets.into_referencing};
    }

   private:
    const T _null_value;
    const pmr_vector<T>& _values;
    const pmr_vector<ChunkOffset>& _end_positions;

    mutable ChunkOffset _prev_chunk_offset;
    mutable size_t _prev_index;
  };
};

}  // namespace opossum
