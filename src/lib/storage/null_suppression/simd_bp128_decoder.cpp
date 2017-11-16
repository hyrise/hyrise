#include "simd_bp128_decoder.hpp"

#include "utils/assert.hpp"


namespace opossum {

void SimdBp128Decoder::ConstIterator::read_meta_info() {
  Packing::read_meta_info(_data->data() + _data_index++, _current_meta_info.data());
  _current_meta_info_index = 0u;
}

void SimdBp128Decoder::ConstIterator::unpack_block() {
  const auto in = _data->data() + _data_index;
  auto out = _current_block->data();
  const auto bit_size = _current_meta_info[_current_meta_info_index++];

  Packing::unpack_block(in, out, bit_size);

  _data_index += bit_size;
  _current_block_index = 0u;
}

}  // namespace opossum
