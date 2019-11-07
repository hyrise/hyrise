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
    if (_segment.null_values()) {
      auto begin = Iterator<ValueIterator>{decompressed_segment.cbegin(), _segment.null_values()->cbegin()};
      auto end = Iterator<ValueIterator>{decompressed_segment.cend(), _segment.null_values()->cend()};
      functor(begin, end);
    } else {
      auto begin = Iterator<ValueIterator>{decompressed_segment.cbegin(), std::nullopt};
      auto end = Iterator<ValueIterator>{decompressed_segment.cend(), std::nullopt};
      functor(begin, end);
    }
  }

  /**
   * For the point access, we first retrieve the values for all chunk offsets in the position list and then save
   * the decompressed values in a vector. The first value in that vector (index 0) is the value for the chunk offset
   * at index 0 in the position list.
   */
  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    using ValueIterator = typename std::vector<T>::const_iterator;

    const auto position_filter_size = position_filter->size();

    // vector storing the uncompressed values
    auto decompressed_filtered_segment = std::vector<ValueType>(position_filter_size);

    // _segment.decompress() takes the currently cached block (reference) and its id in addition to the requested
    // element. If the requested element is not within that block, the next block will be decompressed and written to
    // `cached_block` while the value and the new block id are returned. In case the requested element is within the
    // cached block, the value and the input block id are returned.
    for (auto index = size_t{0u}; index < position_filter_size; ++index) {
      const auto& position = (*position_filter)[index];
      // NOLINTNEXTLINE
      auto [value, block_index] = _segment.decompress(position.chunk_offset, cached_block_index, cached_block);
      decompressed_filtered_segment[index] = std::move(value);
      cached_block_index = block_index;
    }

    if (_segment.null_values()) {
      auto begin =
          PointAccessIterator<ValueIterator>{decompressed_filtered_segment.begin(), _segment.null_values()->cbegin(),
                                             position_filter->cbegin(), position_filter->cbegin()};
      auto end =
          PointAccessIterator<ValueIterator>{decompressed_filtered_segment.begin(), _segment.null_values()->cend(),
                                             position_filter->cbegin(), position_filter->cend()};
      functor(begin, end);
    } else {
      auto begin = PointAccessIterator<ValueIterator>{decompressed_filtered_segment.begin(), std::nullopt,
                                                      position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<ValueIterator>{decompressed_filtered_segment.begin(), std::nullopt,
                                                    position_filter->cbegin(), position_filter->cend()};

      functor(begin, end);
    }
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const LZ4Segment<T>& _segment;
  mutable std::vector<char> cached_block;
  mutable std::optional<size_t> cached_block_index = std::nullopt;

 private:
  template <typename ValueIterator>
  class Iterator : public BaseSegmentIterator<Iterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin and End Iterator
    explicit Iterator(ValueIterator data_it, std::optional<NullValueIterator> null_value_it)
        : _chunk_offset{0u}, _data_it{std::move(data_it)}, _null_value_it{std::move(null_value_it)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
      ++_data_it;
      if (_null_value_it) ++(*_null_value_it);
    }

    void decrement() {
      --_chunk_offset;
      --_data_it;
      if (_null_value_it) --(*_null_value_it);
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
      _data_it += n;
      if (_null_value_it) *_null_value_it += n;
    }

    bool equal(const Iterator& other) const { return _data_it == other._data_it; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const {
      return SegmentPosition<T>{*_data_it, _null_value_it ? **_null_value_it : false, _chunk_offset};
    }

   private:
    ChunkOffset _chunk_offset;
    ValueIterator _data_it;
    std::optional<NullValueIterator> _null_value_it;
  };

  template <typename ValueIterator>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;
    using DataIteratorType = typename std::vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    // Begin Iterator
    PointAccessIterator(DataIteratorType data_it, std::optional<NullValueIterator> null_value_it,
                        PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<ValueIterator>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
          _data_it{std::move(data_it)},
          _null_value_it{std::move(null_value_it)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto& value = *(_data_it + chunk_offsets.offset_in_poslist);
      const auto is_null = _null_value_it && *(*_null_value_it + chunk_offsets.offset_in_referenced_chunk);
      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    DataIteratorType _data_it;
    std::optional<NullValueIterator> _null_value_it;
  };
};

}  // namespace opossum
