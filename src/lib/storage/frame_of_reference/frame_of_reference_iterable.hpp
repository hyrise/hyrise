#pragma once

#include <type_traits>

#include "storage/segment_iterables.hpp"

#include "storage/frame_of_reference_segment.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class FrameOfReferenceIterable : public PointAccessibleSegmentIterable<FrameOfReferenceIterable<T>> {
 public:
  explicit FrameOfReferenceIterable(const FrameOfReferenceSegment<T>& segment) : _segment{segment} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& offset_values) {
      using OffsetValueIteratorT = decltype(offset_values.cbegin());

      auto begin = Iterator<OffsetValueIteratorT>{_segment.block_minima().cbegin(), offset_values.cbegin(),
                                                  _segment.null_values().cbegin()};

      auto end = Iterator<OffsetValueIteratorT>{offset_values.cend()};

      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const PosList& position_filter, const Functor& functor) const {
    resolve_compressed_vector_type(_segment.offset_values(), [&](const auto& vector) {
      auto decompressor = vector.create_decompressor();
      using OffsetValueDecompressorT = std::decay_t<decltype(*decompressor)>;

      auto begin = PointAccessIterator<OffsetValueDecompressorT>{&_segment.block_minima(), &_segment.null_values(),
                                                                 decompressor.get(), position_filter.cbegin(),
                                                                 position_filter.cbegin()};

      auto end = PointAccessIterator<OffsetValueDecompressorT>{position_filter.cbegin(), position_filter.cend()};

      functor(begin, end);
    });
  }

  size_t _on_size() const { return _segment.size(); }

 private:
  const FrameOfReferenceSegment<T>& _segment;

 private:
  template <typename OffsetValueIteratorT>
  class Iterator : public BaseSegmentIterator<Iterator<OffsetValueIteratorT>, SegmentIteratorValue<T>> {
   public:
    using ReferenceFrameIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin Iterator
    explicit Iterator(ReferenceFrameIterator block_minimum_it, OffsetValueIteratorT offset_value_it,
                      NullValueIterator null_value_it)
        : _block_minimum_it{block_minimum_it},
          _offset_value_it{offset_value_it},
          _null_value_it{null_value_it},
          _index_within_frame{0u},
          _chunk_offset{0u} {}

    // End iterator
    explicit Iterator(OffsetValueIteratorT offset_value_it) : Iterator{{}, offset_value_it, {}} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_offset_value_it;
      ++_null_value_it;
      ++_index_within_frame;
      ++_chunk_offset;

      if (_index_within_frame >= FrameOfReferenceSegment<T>::block_size) {
        _index_within_frame = 0u;
        ++_block_minimum_it;
      }
    }

    bool equal(const Iterator& other) const { return _offset_value_it == other._offset_value_it; }

    SegmentIteratorValue<T> dereference() const {
      const auto value = static_cast<T>(*_offset_value_it) + *_block_minimum_it;
      return SegmentIteratorValue<T>{value, *_null_value_it, _chunk_offset};
    }

   private:
    ReferenceFrameIterator _block_minimum_it;
    OffsetValueIteratorT _offset_value_it;
    NullValueIterator _null_value_it;
    size_t _index_within_frame;
    ChunkOffset _chunk_offset;
  };

  template <typename OffsetValueDecompressorT>
  class PointAccessIterator
      : public BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressorT>, SegmentIteratorValue<T>> {
   public:
    // Begin Iterator
    PointAccessIterator(const pmr_vector<T>* block_minima, const pmr_vector<bool>* null_values,
                        OffsetValueDecompressorT* attribute_decompressor,
                        const PosList::const_iterator position_filter_begin, PosList::const_iterator position_filter_it)
        : BasePointAccessSegmentIterator<PointAccessIterator<OffsetValueDecompressorT>,
                                         SegmentIteratorValue<T>>{std::move(position_filter_begin),
                                                                  std::move(position_filter_it)},
          _block_minima{block_minima},
          _null_values{null_values},
          _offset_value_decompressor{attribute_decompressor} {}

    // End Iterator
    explicit PointAccessIterator(const PosList::const_iterator position_filter_begin,
                                 PosList::const_iterator position_filter_it)
        : PointAccessIterator{nullptr, nullptr, nullptr, std::move(position_filter_begin),
                              std::move(position_filter_it)} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    SegmentIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      static constexpr auto block_size = FrameOfReferenceSegment<T>::block_size;

      const auto is_null = (*_null_values)[chunk_offsets.offset_in_referenced_chunk];
      const auto block_minimum = (*_block_minima)[chunk_offsets.offset_in_referenced_chunk / block_size];
      const auto offset_value = _offset_value_decompressor->get(chunk_offsets.offset_in_referenced_chunk);
      const auto value = static_cast<T>(offset_value) + block_minimum;

      return SegmentIteratorValue<T>{value, is_null, chunk_offsets.offset_in_poslist};
    }

   private:
    const pmr_vector<T>* _block_minima;
    const pmr_vector<bool>* _null_values;
    OffsetValueDecompressorT* _offset_value_decompressor;
  };
};

}  // namespace opossum
