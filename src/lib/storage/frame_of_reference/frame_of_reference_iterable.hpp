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
    resolve_compressed_vector_type(_column.offset_values(), [&](const auto& offset_values) {
      using OffsetValueIteratorT = decltype(offset_values.cbegin());

      auto begin = Iterator<OffsetValueIteratorT>{_column.block_minima().cbegin(), offset_values.cbegin(),
                                                  _column.null_values().cbegin()};

      auto end = Iterator<OffsetValueIteratorT>{offset_values.cend()};

      functor(begin, end);
    });
  }

  template <typename Functor>
  void _on_with_iterators(const ChunkOffsetsList& mapped_chunk_offsets, const Functor& functor) const {
    resolve_compressed_vector_type(_column.offset_values(), [&](const auto& vector) {
      auto decoder = vector.create_decoder();
      using OffsetValueDecompressorT = std::decay_t<decltype(*decoder)>;

      auto begin = PointAccessIterator<OffsetValueDecompressorT>{&_column.block_minima(), &_column.null_values(),
                                                                 decoder.get(), mapped_chunk_offsets.cbegin()};

      auto end = PointAccessIterator<OffsetValueDecompressorT>{mapped_chunk_offsets.cend()};

      functor(begin, end);
    });
  }

  size_t _on_size() const { return _column.size(); }

 private:
  const FrameOfReferenceColumn<T>& _column;

 private:
  template <typename OffsetValueIteratorT>
  class Iterator : public BaseColumnIterator<Iterator<OffsetValueIteratorT>, ColumnIteratorValue<T>> {
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

      if (_index_within_frame >= FrameOfReferenceColumn<T>::block_size) {
        _index_within_frame = 0u;
        ++_block_minimum_it;
      }
    }

    bool equal(const Iterator& other) const { return _offset_value_it == other._offset_value_it; }

    ColumnIteratorValue<T> dereference() const {
      const auto value = static_cast<T>(*_offset_value_it) + *_block_minimum_it;
      return ColumnIteratorValue<T>{value, *_null_value_it, _chunk_offset};
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
      : public BasePointAccessColumnIterator<PointAccessIterator<OffsetValueDecompressorT>, ColumnIteratorValue<T>> {
   public:
    // Begin Iterator
    PointAccessIterator(const pmr_vector<T>* block_minima, const pmr_vector<bool>* null_values,
                        OffsetValueDecompressorT* attribute_decoder, ChunkOffsetsIterator chunk_offsets_it)
        : BasePointAccessColumnIterator<PointAccessIterator<OffsetValueDecompressorT>,
                                        ColumnIteratorValue<T>>{chunk_offsets_it},
          _block_minima{block_minima},
          _null_values{null_values},
          _offset_value_decoder{attribute_decoder} {}

    // End Iterator
    explicit PointAccessIterator(ChunkOffsetsIterator chunk_offsets_it)
        : PointAccessIterator{nullptr, nullptr, nullptr, chunk_offsets_it} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    ColumnIteratorValue<T> dereference() const {
      const auto& chunk_offsets = this->chunk_offsets();

      static constexpr auto block_size = FrameOfReferenceColumn<T>::block_size;

      const auto is_null = (*_null_values)[chunk_offsets.into_referenced];
      const auto block_minimum = (*_block_minima)[chunk_offsets.into_referenced / block_size];
      const auto offset_value = _offset_value_decoder->get(chunk_offsets.into_referenced);
      const auto value = static_cast<T>(offset_value) + block_minimum;

      return ColumnIteratorValue<T>{value, is_null, chunk_offsets.into_referencing};
    }

   private:
    const pmr_vector<T>* _block_minima;
    const pmr_vector<bool>* _null_values;
    OffsetValueDecompressorT* _offset_value_decoder;
  };
};

}  // namespace opossum
