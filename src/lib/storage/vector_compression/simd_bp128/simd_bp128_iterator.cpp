#include "simd_bp128_iterator.hpp"

namespace opossum {

SimdBp128Iterator::SimdBp128Iterator(const pmr_vector<uint128_t>* data, size_t size, size_t absolute_index)
    : _data{data},
      _size{size},
      _data_index{0u},
      _absolute_index{absolute_index},
      _current_meta_block{std::make_unique<std::array<uint32_t, Packing::meta_block_size>>()},
      _current_meta_block_index{0u} {
  if (data) {
    _unpack_next_meta_block();
  }
}

SimdBp128Iterator::SimdBp128Iterator(const SimdBp128Iterator& other)
    : _data{other._data},
      _size{other._size},
      _data_index{other._data_index},
      _absolute_index{other._absolute_index},
      _current_meta_block{std::make_unique<std::array<uint32_t, Packing::meta_block_size>>(*other._current_meta_block)},
      _current_meta_block_index{other._current_meta_block_index} {}

void SimdBp128Iterator::_unpack_next_meta_block() {
  _read_meta_info();

  for (auto meta_info_index = 0u; meta_info_index < Packing::blocks_in_meta_block; ++meta_info_index) {
    _unpack_block(meta_info_index);
  }

  _current_meta_block_index = 0u;
}

void SimdBp128Iterator::_read_meta_info() {
  Packing::read_meta_info(_data->data() + _data_index++, _current_meta_info.data());
}

void SimdBp128Iterator::_unpack_block(uint8_t meta_info_index) {
  const auto in = _data->data() + _data_index;
  auto out = _current_meta_block->data() + (meta_info_index * Packing::block_size);
  const auto bit_size = _current_meta_info[meta_info_index];

  Packing::unpack_block(in, out, bit_size);

  _data_index += bit_size;
}

}  // namespace opossum
