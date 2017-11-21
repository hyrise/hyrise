#include "simd_bp128_decoder.hpp"

#include <iostream>

namespace opossum {

void SimdBp128Decoder::_read_meta_info(size_t meta_info_offset) {
  Packing::read_meta_info(_data->data() + meta_info_offset, _cached_meta_info.data());
}

void SimdBp128Decoder::_unpack_block(uint8_t block_index) {
  static const auto meta_info_data_size = 1u;  // One 128 bit block
  const auto relative_data_offset =
      meta_info_data_size + std::accumulate(_cached_meta_info.begin(), _cached_meta_info.begin() + block_index, 0u);
  const auto data_offset = _cached_meta_info_offset + relative_data_offset;
  const auto in = _data->data() + data_offset;
  auto out = _cached_block->data();
  const auto bit_size = _cached_meta_info[block_index];

  Packing::unpack_block(in, out, bit_size);

  _cached_block_first_index = _cached_meta_block_first_index + block_index * Packing::block_size;
}

void SimdBp128Decoder::ConstIterator::_read_meta_info() {
  Packing::read_meta_info(_data->data() + _data_index++, _current_meta_info.data());
  _current_meta_info_index = 0u;
}

void SimdBp128Decoder::ConstIterator::_unpack_block() {
  const auto in = _data->data() + _data_index;
  auto out = _current_block->data();
  const auto bit_size = _current_meta_info[_current_meta_info_index];

  Packing::unpack_block(in, out, bit_size);

  _data_index += bit_size;
  _current_block_index = 0u;
}

}  // namespace opossum
