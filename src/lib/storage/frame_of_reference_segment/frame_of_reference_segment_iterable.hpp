#pragma once

#include <type_traits>

#include "storage/base_segment.hpp"
#include "storage/frame_of_reference_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class FrameOfReferenceSegmentIterable : public PointAccessibleSegmentIterable<FrameOfReferenceSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit FrameOfReferenceSegmentIterable(const FrameOfReferenceSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::AccessType::Sequential] += _segment.size();
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& offset_values) {
      using OffsetValueDecompressor = std::decay_t<decltype(offset_values.create_decompressor())>;

      auto begin = Iterator<OffsetValueDecompressor>{&_segment.block_minima(), &_segment.null_values(),
                                                     offset_values.create_decompressor(), ChunkOffset{0}};

      auto end = Iterator<OffsetValueDecompressor>{&_segment.block_minima(), &_segment.null_values(),
                                                   offset_values.create_decompressor(),
                                                   static_cast<ChunkOffset>(_segment.size())};

      functor(begin, end);
    });
  }

  template <typename Functor, typename PosListType>
  void _on_with_iterators(const std::shared_ptr<PosListType>& position_filter, const Functor& functor) const {
    _segment.access_counter[SegmentAccessCounter::access_type(*position_filter)] += position_filter->size();
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& offset_values) {
      using OffsetValueDecompressor = std::decay_t<decltype(offset_values.create_decompressor())>;
      using PosListIteratorType = std::decay_t<decltype(position_filter->cbegin())>;

      auto begin = PointAccessIterator<OffsetValueDecompressor, PosListIteratorType>{
          &_segment.block_minima(), &_segment.null_values(), offset_values.create_decompressor(),
          position_filter->cbegin(), position_filter->cbegin()};

      auto end = PointAccessIterator<OffsetValueDecompressor, PosListIteratorType>{
          &_segment.block_minima(), &_segment.null_values(), offset_values.create_decompressor(),
          position_filter->cbegin(), position_filter->cend()};

      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const FrameOfReferenceSegment<T>& _segment;

 private:
  template <typename OffsetValueDecompressor>
  class Iterator : public BaseSegmentIterator<Iterator<OffsetValueDecompressor>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = FrameOfReferenceSegmentIterable<T>;

   public:
    explicit Iterator(const pmr_vector<T>* block_minima, const pmr_vector<bool>* null_values,
                      OffsetValueDecompressor offset_value_decompressor, ChunkOffset chunk_offset)
        : _block_minima{block_minima},
          _null_values{null_values},
          _offset_value_decompressor{std::move(offset_value_decompressor)},
          _chunk_offset{chunk_offset} {}

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
      static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

      const auto is_null = (*_null_values)[_chunk_offset];
      const auto block_minimum = (*_block_minima)[_chunk_offset / block_size];
      const auto offset_value = _offset_value_decompressor.get(_chunk_offset);
      const auto value = static_cast<T>(offset_value) + block_minimum;

      return SegmentPosition<T>{value, is_null, _chunk_offset};
    }

   private:
    const pmr_vector<T>* _block_minima;
    const pmr_vector<bool>* _null_values;
    mutable OffsetValueDecompressor _offset_value_decompressor;
    ChunkOffset _chunk_offset;
  };

  template <typename OffsetValueDecompressor, typename PosListIteratorType>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressor, PosListIteratorType>,
                                              SegmentPosition<T>, PosListIteratorType> {
   public:
    using ValueType = T;
    using IterableType = FrameOfReferenceSegmentIterable<T>;

    PointAccessIterator(const pmr_vector<T>* block_minima, const pmr_vector<bool>* null_values,
                        OffsetValueDecompressor offset_value_decompressor, PosListIteratorType position_filter_begin,
                        PosListIteratorType position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressor, PosListIteratorType>,
                                         SegmentPosition<T>, PosListIteratorType>{std::move(position_filter_begin),
                                                                                  std::move(position_filter_it)},
          _block_minima{block_minima},
          _null_values{null_values},
          _offset_value_decompressor{std::move(offset_value_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

      const auto& chunk_offsets = this->chunk_offsets();
      const auto current_offset = chunk_offsets.offset_in_referenced_chunk;

      const auto is_null = (*_null_values)[current_offset];
      const auto block_minimum = (*_block_minima)[current_offset / block_size];
      const auto offset_value = _offset_value_decompressor.get(current_offset);
      const auto value = static_cast<T>(offset_value) + block_minimum;

      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>* _block_minima;
    const pmr_vector<bool>* _null_values;
    mutable OffsetValueDecompressor _offset_value_decompressor;
  };
};

}  // namespace opossum
