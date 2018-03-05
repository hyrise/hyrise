#pragma once

#include <type_traits>

#include "storage/column_iterables.hpp"

#include "storage/frame_of_reference_column.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

template <typename T>
class FrameOfReferenceIterable : public PointAccessibleColumnIterable<FrameOfReferenceIterable<T>> {
 public:
  explicit FrameOfReferenceIterable(const FrameOfReferenceColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(*_column.offset_values(), [&](const auto& offset_values) {
      using ZsIteratorType = decltype(offset_values.cbegin());

      auto begin = Iterator<ZsIteratorType>{_column.reference_frames()->cbegin(), offset_values.cbegin(),
                                            _column.null_values()->cbegin()};

      auto end = Iterator<ZsIteratorType>{offset_values.cend()};

      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    resolve_compressed_vector_type(*_column.offset_values(), [&](const auto& vector) {
      auto decoder = vector.create_decoder();
      using ZsDecoderType = std::decay_t<decltype(*decoder)>;

      auto begin = PointAccessIterator<ZsDecoderType>{_column.reference_frames().get(), _column.null_values().get(), decoder.get(),
                                                      mapped_chunk_offsets.cbegin()};

      auto end = PointAccessIterator<ZsDecoderType>{mapped_chunk_offsets.cend()};

      functor(begin, end);
    });
  }

 private:
  const FrameOfReferenceColumn<T>& _column;

 private:
  template <typename ZsIteratorType>
  class Iterator : public BaseColumnIterator<Iterator<ZsIteratorType>, ColumnIteratorValue<T>> {
   public:
    using ReferenceFrameIterator = typename pmr_vector<T>::const_iterator;
    using OffsetValueIterator = ZsIteratorType;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

   public:
    // Begin Iterator
    explicit Iterator(ReferenceFrameIterator reference_frame_it, OffsetValueIterator offset_value_it,
                      NullValueIterator null_value_it)
        : _reference_frame_it{reference_frame_it},
          _offset_value_it{offset_value_it},
          _null_value_it{null_value_it},
          _index_within_frame{0u},
          _chunk_offset{0u} {}

    // End iterator
    explicit Iterator(OffsetValueIterator offset_value_it) : _offset_value_it{offset_value_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_offset_value_it;
      ++_null_value_it;
      ++_index_within_frame;
      ++_chunk_offset;

      if (_index_within_frame >= FrameOfReferenceColumn<T>::frame_size) {
        _index_within_frame = 0u;
        ++_reference_frame_it;
      }
    }

    bool equal(const Iterator& other) const { return _offset_value_it == other._offset_value_it; }

    ColumnIteratorValue<T> dereference() const {
      const auto value = static_cast<T>(*_offset_value_it) + *_reference_frame_it;
      return ColumnIteratorValue<T>{value, *_null_value_it, _chunk_offset};
    }

   private:
    ReferenceFrameIterator _reference_frame_it;
    OffsetValueIterator _offset_value_it;
    NullValueIterator _null_value_it;
    size_t _index_within_frame;
    ChunkOffset _chunk_offset;
  };

  template <typename ZsDecoderType>
  class PointAccessIterator
      : public BasePointAccessColumnIterator<PointAccessIterator<ZsDecoderType>, ColumnIteratorValue<T>> {
   public:
    // Begin Iterator
    PointAccessIterator(const pmr_vector<T>* reference_frames, const pmr_vector<bool>* null_values,
                        ZsDecoderType* attribute_decoder, ChunkOffsetsIterator chunk_offsets_it)
        : BasePointAccessColumnIterator<PointAccessIterator<ZsDecoderType>, ColumnIteratorValue<T>>{chunk_offsets_it},
          _reference_frames{reference_frames},
          _null_values{null_values},
          _attribute_decoder{attribute_decoder} {}

    // End Iterator
    PointAccessIterator(ChunkOffsetsIterator chunk_offsets_it)
        : BasePointAccessColumnIterator<PointAccessIterator<ZsDecoderType>, ColumnIteratorValue<T>>{chunk_offsets_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      if (chunk_offsets.into_referenced == INVALID_CHUNK_OFFSET)
        return ColumnIteratorValue<T>{T{}, true, chunk_offsets.into_referencing};

      static constexpr auto frame_size = FrameOfReferenceColumn<T>::frame_size;

      const auto is_null = (*_null_values)[chunk_offsets.into_referenced];
      const auto reference_frame = (*_reference_frames)[chunk_offsets.into_referenced / frame_size];
      const auto offset_value = _attribute_decoder->get(chunk_offsets.into_referenced);
      const auto value = static_cast<T>(offset_value) + reference_frame;

      return ColumnIteratorValue<T>{value, is_null, chunk_offsets.into_referencing};
    }

   private:
    const pmr_vector<T>* _reference_frames;
    const pmr_vector<bool>* _null_values;
    ZsDecoderType* _attribute_decoder;
  };
};

}  // namespace opossum
