#pragma once

#include <type_traits>

#include <boost/sort/sort.hpp>

#include "storage/lz4_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"


namespace opossum {

template <typename T>
class LZ4SegmentIterable : public PointAccessibleSegmentIterable<LZ4SegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit LZ4SegmentIterable(const LZ4Segment<T>& segment) : _segment{segment} {}

  // If the LZ4 segment stores strings, the underlying segment will have more blocks than a numeric segment.
  // To maximize the hitting of cached blocks, we sort the pos list before materializing it. This threshold
  // shall determine which (type dependent) number of items is necessary to have blocks at all. If we only
  // expect a single block, no sorting is necessary.
  template <typename T2>
  static size_t get_poslist_sorting_threshold() {
    // Both thresholds have been determined by evluating TPC-H on a recent server machine. Note, both numbers are
    // chosen to avoid worst-case scenarios and do not reflect the actual break-even.
    if constexpr (std::is_same_v<T2, pmr_string>) {
      return size_t{400};
    } else {
      return size_t{1'000};
    }
  }

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    using ValueIterator = typename std::vector<T>::const_iterator;

    auto decompressed_segment = _segment.decompress();
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += decompressed_segment.size();
    if (_segment.null_values()) {
      auto begin =
          Iterator<ValueIterator>{decompressed_segment.cbegin(), _segment.null_values()->cbegin(), ChunkOffset{0u}};
      auto end = Iterator<ValueIterator>{decompressed_segment.cend(), _segment.null_values()->cend(),
                                         static_cast<ChunkOffset>(decompressed_segment.size())};
      functor(begin, end);
    } else {
      auto begin = Iterator<ValueIterator>{decompressed_segment.cbegin(), std::nullopt, ChunkOffset{0u}};
      auto end = Iterator<ValueIterator>{decompressed_segment.cend(), std::nullopt,
                                         static_cast<ChunkOffset>(decompressed_segment.size())};
      functor(begin, end);
    }
  }

  /**
   * For the point access, we first retrieve the values for all chunk offsets in the position list and then save
   * the decompressed values in a vector. The first value in that vector (index 0) is the value for the chunk offset
   * at index 0 in the position list.
   */
  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    const auto position_filter_size = position_filter->size();
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter_size;

    constexpr auto POSITTIONS_TO_READ_FOR_SEQ_ESTIMATION = size_t{100};
    auto pos_list_is_monotonously_increasing = true;
    if (_segment.lz4_blocks().size() > 1) {
      auto previous_position = (*position_filter)[0].chunk_offset;
      for (auto index = size_t{1};  // start at second position
          index < std::min(POSITTIONS_TO_READ_FOR_SEQ_ESTIMATION, position_filter_size)
          && pos_list_is_monotonously_increasing;  // break if non-monotonously increasing
          ++index) {
        const auto current_position = (*position_filter)[index].chunk_offset;
        pos_list_is_monotonously_increasing = current_position >= previous_position;
        previous_position = current_position;
      }
    }

    // vector storing the uncompressed values
    auto decompressed_filtered_segment = std::vector<ValueType>(position_filter_size);
    if (!pos_list_is_monotonously_increasing) {
      // In case the pos list is large, LZ4 can benefit from sorting the pos list as the change of hitting the same
      // decompressed LZ4 block increases. For small pos lists, the sorting causes a unnecessary overhead.
      std::vector<std::pair<RowID, size_t>> position_filter_indexed(position_filter_size);

      for (auto index = size_t{0}; index < position_filter_size; ++index) {
        const auto& row_id = (*position_filter)[index];
        position_filter_indexed[index] = {row_id, index};
      }
      boost::sort::pdqsort(position_filter_indexed.begin(), position_filter_indexed.end(), [](const auto& a, const auto& b) { return a.first < b.first; });

      for (auto index = size_t{0u}; index < position_filter_size; ++index) {
        const auto& position = position_filter_indexed[index].first;
        // NOLINTNEXTLINE
        auto [value, block_index] = _segment.decompress(position.chunk_offset, cached_block_index, cached_block);
        const auto write_position = position_filter_indexed[index].second;
        decompressed_filtered_segment[write_position] = std::move(value);
        cached_block_index = block_index;
      }
    } else {
      for (auto index = size_t{0u}; index < position_filter_size; ++index) {
        const auto& position = (*position_filter)[index];
        // NOLINTNEXTLINE
        auto [value, block_index] = _segment.decompress(position.chunk_offset, cached_block_index, cached_block);
        decompressed_filtered_segment[index] = std::move(value);
        cached_block_index = block_index;
      }
    }

    using PosListIteratorType = decltype(position_filter->cbegin());
    if (_segment.null_values()) {
      auto begin = PointAccessIterator<PosListIteratorType>{decompressed_filtered_segment.begin(),
                                                            _segment.null_values()->cbegin(), position_filter->cbegin(),
                                                            position_filter->cbegin()};
      auto end = PointAccessIterator<PosListIteratorType>{decompressed_filtered_segment.begin(),
                                                          _segment.null_values()->cend(), position_filter->cbegin(),
                                                          position_filter->cend()};

      functor(begin, end);
    } else {
      auto begin = PointAccessIterator<PosListIteratorType>{decompressed_filtered_segment.begin(), std::nullopt,
                                                            position_filter->cbegin(), position_filter->cbegin()};
      auto end = PointAccessIterator<PosListIteratorType>{decompressed_filtered_segment.begin(), std::nullopt,
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
  class Iterator : public AbstractSegmentIterator<Iterator<ValueIterator>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin and End Iterator
    explicit Iterator(ValueIterator data_it, std::optional<NullValueIterator> null_value_it, ChunkOffset chunk_offset)
        : _chunk_offset{chunk_offset}, _data_it{std::move(data_it)}, _null_value_it{std::move(null_value_it)} {}

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

  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
                                                                        SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = LZ4SegmentIterable<T>;
    using DataIteratorType = typename std::vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    // Begin Iterator
    PointAccessIterator(DataIteratorType data_it, std::optional<NullValueIterator> null_value_it,
                        PosListIteratorType position_filter_begin, PosListIteratorType position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>, SegmentPosition<T>,
                                             PosListIteratorType>{std::move(position_filter_begin),
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

