#include "simd_bp128_encoder.hpp"

#include <emmintrin.h>

#include <algorithm>
#include <array>

#include "simd_bp128_vector.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::unique_ptr<BaseZeroSuppressionVector> SimdBp128Encoder::encode(const pmr_vector<uint32_t>& vector,
                                                       const PolymorphicAllocator<size_t>& alloc) {
  init(vector.size());
  for (auto value : vector) append(value);
  finish();

  return std::make_unique<SimdBp128Vector>(std::move(_data), _size);
}

void SimdBp128Encoder::init(size_t size) {
  _data = pmr_vector<uint128_t>((size + 3u) / 4u);
  _data_index = 0u;
  _meta_block_index = 0u;
  _size = size;
}

void SimdBp128Encoder::append(uint32_t value) {
  _pending_meta_block[_meta_block_index++] = value;

  if (meta_block_complete()) {
    pack_meta_block();
  }
}

void SimdBp128Encoder::finish() {
  if (_meta_block_index > 0u) {
    pack_incomplete_meta_block();
  }

  // Resize vector to actual size
  _data.resize(_data_index);
  _data.shrink_to_fit();
}

bool SimdBp128Encoder::meta_block_complete() { return (Packing::meta_block_size - _meta_block_index) <= 0u; }

void SimdBp128Encoder::pack_meta_block() {
  const auto bits_needed = bits_needed_per_block();
  write_meta_info(bits_needed);
  pack_blocks(Packing::blocks_in_meta_block, bits_needed);

  _meta_block_index = 0u;
}

void SimdBp128Encoder::pack_incomplete_meta_block() {
  // Fill remaining elements with zero
  std::fill(_pending_meta_block.begin() + _meta_block_index, _pending_meta_block.end(), 0u);

  const auto bits_needed = bits_needed_per_block();
  write_meta_info(bits_needed);

  // Returns ceiling of integer division
  const auto num_blocks_left = (_meta_block_index + Packing::block_size - 1) / Packing::block_size;

  pack_blocks(num_blocks_left, bits_needed);
}

auto SimdBp128Encoder::bits_needed_per_block() -> std::array<uint8_t, Packing::blocks_in_meta_block> {
  std::array<uint8_t, Packing::blocks_in_meta_block> bits_needed{};

  for (auto block_index = 0u; block_index < Packing::blocks_in_meta_block; ++block_index) {
    const auto block_offset = block_index * Packing::block_size;

    auto bit_collector = uint32_t{0u};
    for (auto index = 0u; index < Packing::block_size; ++index) {
      bit_collector |= _pending_meta_block[block_offset + index];
    }

    for (; bit_collector != 0; bits_needed[block_index]++) {
      bit_collector >>= 1u;
    }
  }

  return bits_needed;
}

void SimdBp128Encoder::write_meta_info(const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed) {
  auto data_ptr = reinterpret_cast<__m128i*>(_data.data());
  Packing::write_meta_info(bits_needed.data(), data_ptr + _data_index);
  ++_data_index;
}

void SimdBp128Encoder::pack_blocks(const uint8_t num_blocks,
                                   const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed) {
  DebugAssert(num_blocks <= 16u, "num_blocks must be smaller than 16.");

  auto in = _pending_meta_block.data();
  for (auto block_index = 0u; block_index < num_blocks; ++block_index) {
    const auto data_ptr = reinterpret_cast<__m128i*>(_data.data());
    const auto out = data_ptr + _data_index;
    Packing::pack_block(in, out, bits_needed[block_index]);

    in += Packing::block_size;
    _data_index += bits_needed[block_index];
  }
}

}  // namespace opossum
