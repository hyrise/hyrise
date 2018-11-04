#include "simd_bp128_compressor.hpp"

#include <algorithm>
#include <array>

#include "simd_bp128_vector.hpp"

#include "utils/assert.hpp"

namespace opossum {

std::unique_ptr<const BaseCompressedVector> SimdBp128Compressor::compress(const pmr_vector<uint32_t>& vector,
                                                                          const PolymorphicAllocator<size_t>& alloc,
                                                                          const UncompressedVectorInfo& meta_info) {
  _init(vector.size(), alloc);
  for (auto value : vector) _append(value);
  _finish();

  return std::make_unique<SimdBp128Vector>(std::move(*_data), _size);
}

std::unique_ptr<BaseVectorCompressor> SimdBp128Compressor::create_new() const {
  return std::make_unique<SimdBp128Compressor>();
}

void SimdBp128Compressor::_init(size_t size, const PolymorphicAllocator<size_t>& alloc) {
  constexpr auto MAX_BIT_SIZE = 32u;

  // Ceiling of integer devision
  const auto div_ceil = [](auto x, auto y) { return (x + y - 1u) / y; };

  const auto num_blocks = div_ceil(size, Packing::block_size) * MAX_BIT_SIZE;
  const auto num_meta_blocks = div_ceil(size, Packing::meta_block_size);
  const auto data_size = num_blocks + num_meta_blocks;

  // Reserve enough memory as the worst case would require (size * 32 bit + meta info)
  _data = std::make_unique<pmr_vector<uint128_t>>(data_size, alloc);
  _data_index = 0u;
  _meta_block_index = 0u;
  _size = size;
}

void SimdBp128Compressor::_append(uint32_t value) {
  _pending_meta_block[_meta_block_index++] = value;

  if (_meta_block_complete()) {
    _pack_meta_block();
  }
}

void SimdBp128Compressor::_finish() {
  if (_meta_block_index > 0u) {
    _pack_incomplete_meta_block();
  }

  // Resize vector to actual size
  _data->resize(_data_index);
  _data->shrink_to_fit();
}

bool SimdBp128Compressor::_meta_block_complete() { return (Packing::meta_block_size - _meta_block_index) <= 0u; }

void SimdBp128Compressor::_pack_meta_block() {
  alignas(16) const auto bits_needed = _bits_needed_per_block();
  _write_meta_info(bits_needed);
  _pack_blocks(Packing::blocks_in_meta_block, bits_needed);

  _meta_block_index = 0u;
}

void SimdBp128Compressor::_pack_incomplete_meta_block() {
  // Fill remaining elements with zero
  std::fill(_pending_meta_block.begin() + _meta_block_index, _pending_meta_block.end(), 0u);

  alignas(16) const auto bits_needed = _bits_needed_per_block();
  _write_meta_info(bits_needed);

  // Returns ceiling of integer division
  const auto num_blocks_left = (_meta_block_index + Packing::block_size - 1) / Packing::block_size;

  _pack_blocks(num_blocks_left, bits_needed);
}

auto SimdBp128Compressor::_bits_needed_per_block() -> std::array<uint8_t, Packing::blocks_in_meta_block> {
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

void SimdBp128Compressor::_write_meta_info(const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed) {
  Packing::write_meta_info(bits_needed.data(), _data->data() + _data_index);
  ++_data_index;
}

void SimdBp128Compressor::_pack_blocks(const uint8_t num_blocks,
                                       const std::array<uint8_t, Packing::blocks_in_meta_block>& bits_needed) {
  DebugAssert(num_blocks <= 16u, "num_blocks must be smaller than 16.");

  auto in = _pending_meta_block.data();
  for (auto block_index = 0u; block_index < num_blocks; ++block_index) {
    const auto out = _data->data() + _data_index;
    Packing::pack_block(in, out, bits_needed[block_index]);

    in += Packing::block_size;
    _data_index += bits_needed[block_index];
  }
}

}  // namespace opossum
