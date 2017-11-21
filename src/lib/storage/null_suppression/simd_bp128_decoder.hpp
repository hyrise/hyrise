#pragma once

#include <emmintrin.h>

#include <array>
#include <limits>
#include <numeric>
#include <functional>
#include <memory>
#include <utility>


#include "ns_decoder.hpp"
#include "simd_bp128_vector.hpp"
#include "simd_bp128_packing.hpp"

#include "types.hpp"


namespace opossum {

class SimdBp128Decoder : public NsDecoder<SimdBp128Decoder> {
 public:
  using Packing = SimdBp128Packing;
  using Vector = SimdBp128Vector;

 public:
  explicit SimdBp128Decoder(const Vector& vector)
      : _data{&vector.data()},
        _size{vector.size()},
        _cached_meta_info_offset{0u},
        _cached_meta_block_first_index{std::numeric_limits<size_t>::max()},
        _cached_block_first_index{std::numeric_limits<size_t>::max()},
        _cached_block{std::make_unique<std::array<uint32_t, Packing::block_size>>()} {}

  uint32_t _on_get(size_t i) {
    if (_is_index_within_cached_block(i)) {
      return _get_within_cached_block(i);
    }

    if (_is_index_within_cached_meta_block(i)) {
      return _get_within_cached_meta_block(i);
    }

    if (_is_index_after_cached_meta_block(i)) {
      const auto relative_index = _index_within_cached_meta_block(i);
      const auto relative_meta_block_index = relative_index / Packing::meta_block_size;

      _read_meta_info_from_offset(relative_meta_block_index);
      return _get_within_cached_meta_block(i);
    }

    _reset_cached_meta_block();
    _read_meta_info(_cached_meta_info_offset);
    const auto meta_block_index = i / Packing::meta_block_size;
    _read_meta_info_from_offset(meta_block_index);
    return _get_within_cached_meta_block(i);
  }

  size_t _on_size() const {
    return _size;
  }

  auto _on_cbegin() const {
    return ConstIterator{_data, _size};
  }

  auto _on_cend() const {
    return ConstIterator{nullptr, _size, _size};
  }

 private:
  bool _is_index_within_cached_block(size_t index) {
    const auto begin = _cached_block_first_index;
    const auto end = _cached_block_first_index + Packing::block_size;
    return begin <= index && index < end;
  }

  size_t _index_within_cached_block(size_t index) {
    return index - _cached_block_first_index;
  }

  bool _is_index_within_cached_meta_block(size_t index) {
    const auto begin = _cached_meta_block_first_index;
    const auto end = _cached_meta_block_first_index + Packing::meta_block_size;
    return begin <= index && index < end;
  }

  size_t _index_within_cached_meta_block(size_t index) {
    return index - _cached_meta_block_first_index;
  }

  bool _is_index_after_cached_meta_block(size_t index) {
    return (_cached_meta_block_first_index + Packing::meta_block_size) <= index;
  }

  uint32_t _get_within_cached_block(size_t index) {
    return (*_cached_block)[_index_within_cached_block(index)];
  }

  uint32_t _get_within_cached_meta_block(size_t index) {
    const auto block_index = _index_within_cached_meta_block(index) / Packing::block_size;
    _unpack_block(block_index);

    return (*_cached_block)[_index_within_cached_block(index)];
  }

  void _read_meta_info_from_offset(size_t meta_block_index) {
    auto meta_info_offset = _cached_meta_info_offset;
    for (auto i = 0u; i < meta_block_index; ++i) {
      static const auto meta_info_data_size = 1u;  // One 128 bit block
      const auto meta_block_data_size = meta_info_data_size + std::accumulate(_cached_meta_info.begin(), _cached_meta_info.end(), 0u);
      meta_info_offset += meta_block_data_size;
      _read_meta_info(meta_info_offset);
    }

    _cached_meta_info_offset = meta_info_offset;
    _cached_meta_block_first_index += meta_block_index * Packing::meta_block_size;
  }

  void _reset_cached_meta_block() {
    _cached_meta_info_offset = 0u;
    _cached_meta_block_first_index = 0u;
  }

  void _read_meta_info(size_t meta_info_offset);
  void _unpack_block(uint8_t block_index);

 private:
  const pmr_vector<__m128i>* _data;
  const size_t _size;

  size_t _cached_meta_info_offset;
  size_t _cached_meta_block_first_index;
  std::array<uint8_t, Packing::blocks_in_meta_block> _cached_meta_info;

  size_t _cached_block_first_index;
  const std::unique_ptr<std::array<uint32_t, Packing::block_size>> _cached_block;

 public:
  class ConstIterator : public BaseNsIterator<ConstIterator> {
   public:
    ConstIterator(const pmr_vector<__m128i>* data, size_t size, size_t absolute_index = 0u)
        : _data{data},
          _size{size},
          _data_index{0u},
          _absolute_index{absolute_index},
          _current_meta_info_index{0u},
          _current_block{std::make_unique<std::array<uint32_t, Packing::block_size>>()},
          _current_block_index{0u} {
      if (data) {
        _read_meta_info();
        _unpack_block();
      }
    }

    ConstIterator(const ConstIterator& other)
        : _data{other._data},
          _size{other._size},
          _data_index{other._data_index},
          _absolute_index{other._absolute_index},
          _current_meta_info{other._current_meta_info},
          _current_meta_info_index{other._current_meta_info_index},
          _current_block{std::make_unique<std::array<uint32_t, Packing::block_size>>(*other._current_block)},
          _current_block_index{other._current_block_index} {}


    ConstIterator(ConstIterator&& other) = default;
    ~ConstIterator() = default;

   private:
    friend class boost::iterator_core_access;  // grants the boost::iterator_facade access to the private interface

    void increment() {
      ++_absolute_index;
      ++_current_block_index;

      if (_current_block_index >= Packing::block_size && _absolute_index < _size) {
        ++_current_meta_info_index;

        if (_current_meta_info_index >= Packing::blocks_in_meta_block) {
          _read_meta_info();
          _unpack_block();
        } else {
          _unpack_block();
        }
      }
    }

    bool equal(const ConstIterator& other) const { return _absolute_index == other._absolute_index; }

    uint32_t dereference() const {
      return (*_current_block)[_current_block_index];
    }

   private:
    void _read_meta_info();
    void _unpack_block();

   private:
    const pmr_vector<__m128i>* _data;
    const size_t _size;

    size_t _data_index;
    size_t _absolute_index;

    std::array<uint8_t, Packing::blocks_in_meta_block> _current_meta_info;
    size_t _current_meta_info_index;

    const std::unique_ptr<std::array<uint32_t, Packing::block_size>> _current_block;
    size_t _current_block_index;
  };
};

}  // namespace opossum
