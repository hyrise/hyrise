#pragma once

#include <algorithm>

#include "include/codecfactory.h" // todo

#include "storage/segment_iterables.hpp"
#include "storage/simdCAISegment.hpp"

#include "utils/performance_warning.hpp"

namespace opossum {

template <typename T>
class SIMDCAISegmentIterable : public PointAccessibleSegmentIterable<SIMDCAISegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit SIMDCAISegmentIterable(const SIMDCAISegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    auto begin = Iterator{_segment.encoded_values(), _segment.null_values(), 0, ChunkOffset{0}};
    auto end = Iterator{_segment.encoded_values(), _segment.null_values(), 0, static_cast<ChunkOffset>(_segment.size())};

    functor(begin, end);
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();

    using PosListIteratorType = decltype(position_filter->cbegin());
    auto begin =
        PointAccessIterator<PosListIteratorType>{_segment.encoded_values(), _segment.null_values(), 0,
                                                 position_filter->cbegin(), position_filter->cbegin()};
    auto end =
        PointAccessIterator<PosListIteratorType>{_segment.encoded_values(), _segment.null_values(), 0,
                                                 position_filter->cbegin(), position_filter->cend()};
    functor(begin, end);
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const SIMDCaiSegment<T>& _segment;

 private:
  class Iterator : public AbstractSegmentIterator<Iterator, SegmentPosition<T>> {
    public:
    using ValueType = T;
    using IterableType = SIMDCAISegmentIterable<T>;
    using EndPositionIterator = typename pmr_vector<ChunkOffset>::const_iterator;

    public:
    explicit Iterator(const std::shared_ptr<const pmr_vector<uint32_t>>& encoded_values,
                      const std::optional<const pmr_vector<bool>>& null_values,
                      const uint8_t &codec_id,
                      ChunkOffset chunk_offset)
        : _encoded_values{encoded_values},
          _null_values{null_values},
          _chunk_offset{chunk_offset},
          _codec_id{codec_id} {

      _decoded_values = std::vector<uint32_t>(_null_values->size());
      size_t recovered_size = _decoded_values.size();
      FastPForLib::IntegerCODEC &codec = *FastPForLib::CODECFactory::getFromName("simdbinarypacking"); // todo
      codec.decodeArray(_encoded_values->data(), _encoded_values->size(), _decoded_values.data(), recovered_size);
    }

    private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_chunk_offset;
    }

    void decrement() {
      DebugAssert(_chunk_offset > 0,
                  "An iterator pointing at the begin of a segment cannot be decremented, see "
                  "https://eel.is/c++draft/iterator.concept.bidir (iterator can be decremented if "
                  "a dereferencable iterator value precedes it).");
      --_chunk_offset;
    }

    void advance(std::ptrdiff_t n) {
      _chunk_offset += n;
    }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return static_cast<std::ptrdiff_t>(other._chunk_offset) - _chunk_offset;
    }

    SegmentPosition<T> dereference() const {
      const auto is_null = (*_null_values)[_chunk_offset];
      const auto value = static_cast<T>(_decoded_values[_chunk_offset]);
      return SegmentPosition<T>{value, is_null, _chunk_offset};
    }

    private:
    std::shared_ptr<const pmr_vector<uint32_t>> _encoded_values;
    const std::optional<const pmr_vector<bool>> _null_values;
    const uint8_t _codec_id;
    std::vector<uint32_t> _decoded_values;

    ChunkOffset _chunk_offset;
  };


  template <typename PosListIteratorType>
  class PointAccessIterator : public AbstractPointAccessSegmentIterator<PointAccessIterator<PosListIteratorType>,
      SegmentPosition<T>, PosListIteratorType> {
    public:
    using ValueType = T;
    using IterableType = SIMDCAISegmentIterable<T>;

    explicit PointAccessIterator(const std::shared_ptr<const pmr_vector<uint32_t>>& encoded_values,
                                 const std::shared_ptr<const pmr_vector<bool>>& null_values,
                                 const uint8_t &codec_id,
                                 const PosListIteratorType position_filter_begin,
                                 PosListIteratorType&& position_filter_it)
        : AbstractPointAccessSegmentIterator<PointAccessIterator, SegmentPosition<T>,
          PosListIteratorType>{std::move(position_filter_begin),
          std::move(position_filter_it)},
    _encoded_values{encoded_values},
        _null_values{null_values},
        _codec_id{codec_id} {

      _decoded_values = std::vector<uint32_t>(_null_values->size());
      size_t recovered_size = _decoded_values.size();
      FastPForLib::IntegerCODEC &codec = *FastPForLib::CODECFactory::getFromName("simdbinarypacking");
      codec.decodeArray(_encoded_values->data(), _encoded_values->size(), _decoded_values.data(), recovered_size);
    }

    private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      const auto current_offset = chunk_offsets.offset_in_referenced_chunk;

      const auto is_null = (*_null_values)[current_offset];
      const auto value = static_cast<T>(_decoded_values[current_offset]);
      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

    private:
    std::shared_ptr<const pmr_vector<uint32_t>> _encoded_values;
    std::shared_ptr<const pmr_vector<bool>> _null_values;
    const uint8_t _codec_id;
    std::vector<uint32_t> _decoded_values;
  };
};

}  // namespace opossum