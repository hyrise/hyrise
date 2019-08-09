#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/lz4_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class LZ4SegmentIterable : public PointAccessibleSegmentIterable<LZ4SegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit LZ4SegmentIterable(const LZ4Segment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    using ValueIterator = typename std::vector<T>::const_iterator;

    auto decompressed_segment = _segment.decompress();

    /**
     * If the null value vector doesn't exist, then the segment does not have any row value that is null. In that case,
     * we can just use a default initialized boolean vector.
     */
    const auto null_values = _segment.null_values() ? *_segment.null_values() : pmr_vector<bool>(_segment.size());

    auto begin = Iterator<ValueIterator>{decompressed_segment.cbegin(), null_values.cbegin()};
    auto end = Iterator<ValueIterator>{decompressed_segment.cend(), null_values.cend()};

    functor(begin, end);
  }

  /**
   * For the point access, we first retrieve the values for all chunk offsets in the position list and then save
   * the decompressed values in a vector. The first value in that vector (index 0) is the value for the chunk offset
   * at index 0 in the position list.
   */
  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    using ValueIterator = typename std::vector<T>::const_iterator;

    auto decompressed_filtered_segment = std::vector<ValueType>(position_filter->size());
    auto cached_block = std::vector<char>{};
    auto cached_block_index = std::optional<size_t>{};
    for (auto index = size_t{0u}; index < position_filter->size(); ++index) {
      const auto& position = (*position_filter)[index];
      // NOLINTNEXTLINE
      auto [value, block_index] = _segment.decompress(position.chunk_offset, cached_block_index, cached_block);
      decompressed_filtered_segment[index] = std::move(value);
      cached_block_index = block_index;
    }

    auto begin = PointAccessIterator<ValueIterator>{decompressed_filtered_segment, &_segment.null_values(),
                                                    position_filter->cbegin(), position_filter->cbegin()};
    auto end = PointAccessIterator<ValueIterator>{decompressed_filtered_segment, &_segment.null_values(),
                                                  position_filter->cbegin(), position_filter->cend()};

    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const LZ4Segment<T>& _segment;

 private:
  template <typename ValueIterator>
  class Iterator : public BaseSegmentIterator<Iterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin and End Iterator
    explicit Iterator(ValueIterator data_it, const NullValueIterator null_value_it)
        : _chunk_offset{0u}, _data_it{data_it}, _null_value_it{null_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      ++_data_it;
      ++_null_value_it;
    }

    void decrement() {
      --_chunk_offset;
      --_data_it;
      --_null_value_it;
    }

    void advance(std::ptrdiff_t n) {
      // The easy way for now
      if (n < 0) {
        for (std::ptrdiff_t i = n; i < 0; ++i) {
          decrement();
        }
      } else {
        for (std::ptrdiff_t i = 0; i < n; ++i) {
          increment();
        }
      }
    }

    bool equal(const Iterator& other) const { return _data_it == other._data_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const { return SegmentPosition<T>{*_data_it, *_null_value_it, _chunk_offset}; }

   private:
    ChunkOffset _chunk_offset;
    ValueIterator _data_it;
    NullValueIterator _null_value_it;
  };

  template <typename ValueIterator>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;

    // Begin Iterator
    PointAccessIterator(const std::vector<T>& data, const std::optional<pmr_vector<bool>>* null_values,
                        const PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<ValueIterator>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
          _data{data},
          _null_values{null_values} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto& value = _data[chunk_offsets.offset_in_poslist];
      const auto is_null = *_null_values && (**_null_values)[chunk_offsets.offset_in_referenced_chunk];
      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const std::vector<T> _data;
    const std::optional<pmr_vector<bool>>* _null_values;
  };
};

}  // namespace opossum
