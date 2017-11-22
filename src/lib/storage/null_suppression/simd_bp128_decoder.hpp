#pragma once

#include <emmintrin.h>

#include <array>
#include <functional>
#include <limits>
#include <memory>
#include <numeric>
#include <utility>

#include "base_ns_decoder.hpp"
#include "simd_bp128_iterator.hpp"
#include "simd_bp128_packing.hpp"
#include "simd_bp128_vector.hpp"

#include "types.hpp"

namespace opossum {

class SimdBp128Decoder : public NsDecoder<SimdBp128Decoder> {
 public:
  using Packing = SimdBp128Packing;
  using Vector = SimdBp128Vector;
  using Iterator = SimdBp128Iterator;

 public:
  explicit SimdBp128Decoder(const Vector& vector);
  SimdBp128Decoder(const SimdBp128Decoder& other);

  SimdBp128Decoder(SimdBp128Decoder&& other) = default;
  ~SimdBp128Decoder() = default;

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

  size_t _on_size() const { return _size; }

  auto _on_cbegin() const { return Iterator{_data, _size, 0u}; }

  auto _on_cend() const { return Iterator{nullptr, _size, _size}; }

 private:
  bool _is_index_within_cached_block(size_t index) {
    const auto begin = _cached_block_first_index;
    const auto end = _cached_block_first_index + Packing::block_size;
    return begin <= index && index < end;
  }

  size_t _index_within_cached_block(size_t index) { return index - _cached_block_first_index; }

  bool _is_index_within_cached_meta_block(size_t index) {
    const auto begin = _cached_meta_block_first_index;
    const auto end = _cached_meta_block_first_index + Packing::meta_block_size;
    return begin <= index && index < end;
  }

  size_t _index_within_cached_meta_block(size_t index) { return index - _cached_meta_block_first_index; }

  bool _is_index_after_cached_meta_block(size_t index) {
    return (_cached_meta_block_first_index + Packing::meta_block_size) <= index;
  }

  uint32_t _get_within_cached_block(size_t index) { return (*_cached_block)[_index_within_cached_block(index)]; }

  uint32_t _get_within_cached_meta_block(size_t index) {
    const auto block_index = _index_within_cached_meta_block(index) / Packing::block_size;
    _unpack_block(block_index);

    return (*_cached_block)[_index_within_cached_block(index)];
  }

  void _read_meta_info_from_offset(size_t meta_block_index) {
    auto meta_info_offset = _cached_meta_info_offset;
    for (auto i = 0u; i < meta_block_index; ++i) {
      static const auto meta_info_data_size = 1u;  // One 128 bit block
      const auto meta_block_data_size =
          meta_info_data_size + std::accumulate(_cached_meta_info.begin(), _cached_meta_info.end(), 0u);
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
};

}  // namespace opossum
