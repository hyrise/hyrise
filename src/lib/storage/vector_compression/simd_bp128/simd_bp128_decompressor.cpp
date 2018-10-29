#include "simd_bp128_decompressor.hpp"

#include "simd_bp128_vector.hpp"

namespace opossum {

SimdBp128Decompressor::SimdBp128Decompressor(const SimdBp128Vector& vector)
    : _data{&vector._data},
      _size{vector._size},
      _cached_meta_info_offset{0u},
      _cached_meta_block_first_index{std::numeric_limits<size_t>::max()},
      _cached_block_first_index{std::numeric_limits<size_t>::max()},
      _cached_block{std::make_unique<std::array<uint32_t, Packing::block_size>>()} {}

SimdBp128Decompressor::SimdBp128Decompressor(const SimdBp128Decompressor& other)
    : _data{other._data},
      _size{other._size},
      _cached_meta_info_offset{other._cached_meta_info_offset},
      _cached_meta_block_first_index{other._cached_meta_block_first_index},
      _cached_block_first_index{other._cached_block_first_index},
      _cached_meta_info{other._cached_meta_info},
      _cached_block{std::make_unique<std::array<uint32_t, Packing::block_size>>(*other._cached_block)} {}

void SimdBp128Decompressor::_read_meta_info(size_t meta_info_offset) {
  Packing::read_meta_info(_data->data() + meta_info_offset, _cached_meta_info.data());
}

void SimdBp128Decompressor::_unpack_block(uint8_t block_index) {
  static const auto meta_info_data_size = 1u;  // One 128 bit block

  // Calculate data offset relative to the current _cached_meta_info_offset
  const auto relative_data_offset =
      meta_info_data_size + std::accumulate(_cached_meta_info.begin(), _cached_meta_info.begin() + block_index, 0u);

  // Absolute data offset within compressed vector
  const auto data_offset = _cached_meta_info_offset + relative_data_offset;

  const auto compressed_data_in = _data->data() + data_offset;
  auto decompressed_data_out = _cached_block->data();
  const auto bit_size = _cached_meta_info[block_index];

  Packing::unpack_block(compressed_data_in, decompressed_data_out, bit_size);

  _cached_block_first_index = _cached_meta_block_first_index + block_index * Packing::block_size;
}

}  // namespace opossum
