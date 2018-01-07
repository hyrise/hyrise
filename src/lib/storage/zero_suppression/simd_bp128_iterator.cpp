#include "simd_bp128_iterator.hpp"

#include <emmintrin.h>

namespace opossum {

SimdBp128Iterator::SimdBp128Iterator(const pmr_vector<uint128_t>* data, size_t size, size_t absolute_index)
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

SimdBp128Iterator::SimdBp128Iterator(const SimdBp128Iterator& other)
    : _data{other._data},
      _size{other._size},
      _data_index{other._data_index},
      _absolute_index{other._absolute_index},
      _current_meta_info{other._current_meta_info},
      _current_meta_info_index{other._current_meta_info_index},
      _current_block{std::make_unique<std::array<uint32_t, Packing::block_size>>(*other._current_block)},
      _current_block_index{other._current_block_index} {}

void SimdBp128Iterator::_read_meta_info() {
  Packing::read_meta_info(_data->data() + _data_index++, _current_meta_info.data());
  _current_meta_info_index = 0u;
}

void SimdBp128Iterator::_unpack_block() {
  const auto in = _data->data() + _data_index;
  auto out = _current_block->data();
  const auto bit_size = _current_meta_info[_current_meta_info_index];

  Packing::unpack_block(in, out, bit_size);

  _data_index += bit_size;
  _current_block_index = 0u;
}

}  // namespace opossum
