#pragma once

#include <type_traits>

#include <emmintrin.h>

#include "storage/column_iterables.hpp"

#include "storage/frame_of_reference_column.hpp"
#include "storage/vector_compression/resolve_compressed_vector_type.hpp"

namespace opossum {

namespace detail {

template <typename OffsetValueIteratorT>
class SimdIterator : public BaseColumnIterator<SimdIterator<OffsetValueIteratorT>, ColumnIteratorValue<int32_t>> {
 public:
  using ValueT = int32_t;

  static constexpr auto block_size = FrameOfReferenceColumn<ValueT>::block_size;

  using BlockMinimumIterator = typename pmr_vector<ValueT>::const_iterator;
  using NullValueIterator = typename pmr_vector<bool>::const_iterator;
  using CurrentBlock = std::array<ValueT, block_size>;
  using CurrentBlockIterator = typename CurrentBlock::const_iterator;

 public:
  // Begin Iterator
  explicit SimdIterator(BlockMinimumIterator block_minimum_begin, NullValueIterator null_value_begin,
                        OffsetValueIteratorT offset_value_begin, OffsetValueIteratorT offset_value_end)
      : _block_minimum_it{block_minimum_begin},
        _null_value_it{null_value_begin},
        _offset_value_it{offset_value_begin},
        _offset_value_end{offset_value_end},
        _chunk_offset{0u} {
    _decode_next_block();
  }

  // End iterator
  explicit SimdIterator(NullValueIterator null_value_end, OffsetValueIteratorT offset_value_begin,
                        OffsetValueIteratorT offset_value_end)
      : _null_value_it{null_value_end},
        _offset_value_it{offset_value_begin},
        _offset_value_end{offset_value_end},
        _chunk_offset{0u} {}

 private:
  friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

  void increment() {
    ++_null_value_it;
    ++_current_block_it;
    ++_chunk_offset;

    if (_current_block_it == _current_block.cend()) {
      _decode_next_block();
    }
  }

  bool equal(const SimdIterator& other) const { return _null_value_it == other._null_value_it; }

  ColumnIteratorValue<ValueT> dereference() const {
    return {*_current_block_it, *_null_value_it, _chunk_offset};
  }

 private:
  void _decode_next_block() {
    auto current_block_it = _current_block.begin();
    for (; current_block_it != _current_block.end() && _offset_value_it != _offset_value_end; ++current_block_it, ++_offset_value_it) {
      *current_block_it = static_cast<ValueT>(*_offset_value_it);
    }

    const auto curret_block_end = current_block_it;
    const auto current_block_size = std::distance(_current_block.begin(), curret_block_end);

    // Ceiling of integer division
    // static const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

    static constexpr auto num_simd_width = 4u;

    auto minimum_reg = _mm_set1_epi32(*_block_minimum_it);
    ++_block_minimum_it;

    for (auto i = 0u; i < current_block_size; i += num_simd_width) {
      auto data_ptr = reinterpret_cast<__m128i*>(_current_block.data() + i);

      auto offset_value_reg = _mm_loadu_si128(data_ptr);
      auto value_reg = _mm_add_epi32(minimum_reg, offset_value_reg);
      _mm_storeu_si128(data_ptr, value_reg);
    }

    _current_block_it = _current_block.cbegin();
  }

 private:
  BlockMinimumIterator _block_minimum_it;
  NullValueIterator _null_value_it;
  OffsetValueIteratorT _offset_value_it;
  OffsetValueIteratorT _offset_value_end;
  ChunkOffset _chunk_offset;
  CurrentBlock _current_block;
  CurrentBlockIterator _current_block_it;
};

}  // detail

template <typename T>
class FrameOfReferenceIterable : public PointAccessibleColumnIterable<FrameOfReferenceIterable<T>> {
 public:
  explicit FrameOfReferenceIterable(const FrameOfReferenceColumn<T>& column) : _column{column} {}

  template <typename Functor>
  void _on_with_iterators(const Functor& functor) const {
    resolve_compressed_vector_type(_column.offset_values(), [&](const auto& offset_values) {
      using OffsetValueIteratorT = decltype(offset_values.cbegin());

      using IteratorT = std::conditional_t<std::is_same_v<int32_t, T>, detail::SimdIterator<OffsetValueIteratorT>, Iterator<OffsetValueIteratorT>>;

      auto begin = IteratorT{_column.block_minima().cbegin(), _column.null_values().cbegin(),
                             offset_values.cbegin(), offset_values.cend()};

      auto end = IteratorT{_column.null_values().cend(), offset_values.cbegin(), offset_values.cend()};

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

 private:
  const FrameOfReferenceColumn<T>& _column;

 private:
  template <typename OffsetValueIteratorT>
  class Iterator : public BaseColumnIterator<Iterator<OffsetValueIteratorT>, ColumnIteratorValue<T>> {
   public:
    using BlockMinimumIterator = typename pmr_vector<T>::const_iterator;
    using NullValueIterator = typename pmr_vector<bool>::const_iterator;

    static constexpr auto block_size = FrameOfReferenceColumn<T>::block_size;

   public:
    // Begin Iterator
    explicit Iterator(BlockMinimumIterator block_minimum_begin, NullValueIterator null_value_begin,
                      OffsetValueIteratorT offset_value_begin, OffsetValueIteratorT offset_value_end)
        : _block_minimum_it{block_minimum_begin},
          _offset_value_it{offset_value_begin},
          _null_value_it{null_value_begin},
          _index_within_frame{0u},
          _chunk_offset{0u} {}

    // End iterator
    explicit Iterator(NullValueIterator null_value_end, OffsetValueIteratorT offset_value_begin,
                      OffsetValueIteratorT offset_value_end)
        : Iterator{{}, null_value_end, offset_value_begin, offset_value_end} {}

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_offset_value_it;
      ++_null_value_it;
      ++_index_within_frame;
      ++_chunk_offset;

      if (_index_within_frame >= block_size) {
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
    BlockMinimumIterator _block_minimum_it;
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
