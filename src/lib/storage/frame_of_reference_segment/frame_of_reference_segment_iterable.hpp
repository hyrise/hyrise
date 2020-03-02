#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/frame_of_reference_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class FrameOfReferenceSegmentIterable : public PointAccessibleSegmentIterable<FrameOfReferenceSegmentIterable<T>> {
 public:
  using ValueType = T;

  explicit FrameOfReferenceSegmentIterable(const FrameOfReferenceSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& offset_values) {
      using OffsetValueIteratorT = decltype(offset_values.cbegin());

      auto begin = Iterator<OffsetValueIteratorT>{_segment.block_minima().cbegin(), offset_values.cbegin(),
                                                  _segment.null_values().cbegin(), ChunkOffset{0}};

      auto end =
          Iterator<OffsetValueIteratorT>{_segment.block_minima().cbegin(), offset_values.cbegin(),
                                         _segment.null_values().cbegin(), static_cast<ChunkOffset>(_segment.size())};

      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const std::shared_ptr<const PosList>& position_filter, const Functor& functor) const {
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& vector) {
      using OffsetValueDecompressorT = std::decay_t<decltype(vector.create_decompressor())>;

      auto begin = PointAccessIterator<OffsetValueDecompressorT>{
          _segment.block_minima().cbegin(), _segment.null_values().cbegin(), vector.create_decompressor(),
          position_filter->cbegin(), position_filter->cbegin()};

      auto end = PointAccessIterator<OffsetValueDecompressorT>{
          _segment.block_minima().cbegin(), _segment.null_values().cbegin(), vector.create_decompressor(),
          position_filter->cbegin(), position_filter->cend()};

      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const FrameOfReferenceSegment<T>& _segment;

 private:
  template <typename OffsetValueIteratorT>
  class Iterator : public BaseSegmentIterator<Iterator<OffsetValueIteratorT>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = FrameOfReferenceSegmentIterable<T>;
    using ReferenceFrameIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    explicit Iterator(ReferenceFrameIterator block_minimum_it, OffsetValueIteratorT offset_value_it,
                      NullValueIterator null_value_it, ChunkOffset chunk_offset)
        : _block_minimum_it{std::move(block_minimum_it)},
          _offset_value_it{std::move(offset_value_it)},
          _null_value_it{std::move(null_value_it)},
          _chunk_offset{chunk_offset} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() { ++_chunk_offset; }

    void decrement() { --_chunk_offset; }

    void advance(std::ptrdiff_t n) { _chunk_offset += n; }

    bool equal(const Iterator& other) const { return _chunk_offset == other._chunk_offset; }

    std::ptrdiff_t distance_to(const Iterator& other) const {
      return std::ptrdiff_t{other._chunk_offset} - std::ptrdiff_t{_chunk_offset};
    }

    SegmentPosition<T> dereference() const {
      static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

      const auto block_minimum = *(_block_minimum_it + (_chunk_offset / block_size));
      const auto value = static_cast<T>(*(_offset_value_it + _chunk_offset)) + block_minimum;
      return SegmentPosition<T>{value, *(_null_value_it + _chunk_offset), _chunk_offset};
    }

   private:
    ReferenceFrameIterator _block_minimum_it;
    OffsetValueIteratorT _offset_value_it;
    NullValueIterator _null_value_it;
    ChunkOffset _chunk_offset;
  };

  template <typename OffsetValueDecompressorT>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressorT>, SegmentPosition<T>> {
   public:
    using ValueType = T;
    using IterableType = FrameOfReferenceSegmentIterable<T>;
    using ReferenceFrameIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    // Begin Iterator
    PointAccessIterator(ReferenceFrameIterator block_minimum_it, NullValueIterator null_value_it,
                        std::optional<OffsetValueDecompressorT> attribute_decompressor,
                        PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressorT>,
                                         SegmentPosition<T>>{std::move(position_filter_begin),
                                                             std::move(position_filter_it)},
          _block_minimum_it{std::move(block_minimum_it)},
          _null_value_it{std::move(null_value_it)},
          _offset_value_decompressor{std::move(attribute_decompressor)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentPosition<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();
      static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

      const auto current_offset = chunk_offsets.offset_in_referenced_chunk;

      const auto is_null = *(_null_value_it + current_offset);
      const auto block_minimum = *(_block_minimum_it + (current_offset / block_size));
      const auto offset_value = _offset_value_decompressor->get(current_offset);
      const auto value = static_cast<T>(offset_value) + block_minimum;

      return SegmentPosition<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    ReferenceFrameIterator _block_minimum_it;
    NullValueIterator _null_value_it;
    mutable std::optional<OffsetValueDecompressorT> _offset_value_decompressor;
  };
};

}  // namespace opossum
