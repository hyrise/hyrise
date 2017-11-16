#include "simd_bp128_encoder.hpp"

#include <algorithm>
#include <array>

#include "utils/assert.hpp"


namespace {

template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Pack128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;
    constexpr auto i_max = (_32_bit - carry_over) / bit_size;

    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = carry_over + i * bit_size;
      in_reg = _mm_and_si128(_mm_loadu_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset));
    }

    constexpr auto next_offset = carry_over + i_max * bit_size;
    constexpr auto num_first_bits = _32_bit - next_offset;
    if (next_offset < _32_bit) {
      in_reg = _mm_and_si128(_mm_loadu_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, next_offset));

      _mm_storeu_si128(out, out_reg);
      ++out;

      out_reg = _mm_srli_epi32(in_reg, num_first_bits);
    } else {
      _mm_storeu_si128(out, out_reg);
      ++out;

      out_reg = _mm_setzero_si128();
    }

    constexpr auto new_carry_over = next_offset < _32_bit ? bit_size - num_first_bits : 0u;
    Pack128Bit<bit_size, new_carry_over, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Pack128Bit<bit_size, carry_over, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

}  // namespace

namespace opossum {

void SimdBp128Encoder::init(size_t size) {
  _data = pmr_vector<__m128i>((size + 3u) / 4u);
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

std::unique_ptr<BaseEncodedVector> SimdBp128Encoder::get_vector() {
  return std::make_unique<SimdBp128Vector>(std::move(_data), _size);
}

bool SimdBp128Encoder::meta_block_complete() {
  return (Vector::meta_block_size - _meta_block_index) <= 1u;
}

void SimdBp128Encoder::pack_meta_block() {
  const auto bits_needed = bits_needed_per_block();
  write_meta_info(bits_needed);
  pack_blocks(Vector::blocks_in_meta_block, bits_needed);

  _meta_block_index = 0u;
}

void SimdBp128Encoder::pack_incomplete_meta_block() {
  // Fill remaining elements with zero
  std::fill(_pending_meta_block.begin() + _meta_block_index, _pending_meta_block.end(), 0u);

  const auto bits_needed = bits_needed_per_block();
  write_meta_info(bits_needed);

  // Returns ceiling of integer division
  const auto num_blocks_left = (_meta_block_index + Vector::block_size - 1) / Vector::block_size;

  pack_blocks(num_blocks_left, bits_needed);
}

auto SimdBp128Encoder::bits_needed_per_block() -> std::array<uint8_t, Vector::blocks_in_meta_block> {
  std::array<uint8_t, Vector::blocks_in_meta_block> bits_needed{};

  for (auto block_index = 0u; block_index < Vector::blocks_in_meta_block; ++block_index) {
    const auto block_offset = block_index * Vector::block_size;

    auto bit_collector = uint32_t{0u};
    for (auto index = 0u; index < Vector::block_size; ++index) {
      bit_collector |= _pending_meta_block[block_offset + index];
    }

    for (;bit_collector != 0; bits_needed[block_index]++) { bit_collector >>= 1u; }
  }

  return bits_needed;
}

void SimdBp128Encoder::write_meta_info(const std::array<uint8_t, blocks_in_meta_block>& bits_needed) {
  const auto meta_block_info = _mm_loadu_si128(reinterpret_cast<const __m128i*>(bits_needed.data()));
  _mm_storeu_si128(_data.data() + _data_index++, meta_block_info);
}

void SimdBp128Encoder::pack_blocks(const uint8_t num_blocks,
                                   const std::array<uint8_t, Vector::blocks_in_meta_block>& bits_needed) {
  DebugAssert(num_blocks <= 16u, "num_blocks must be smaller than 16.");

  auto in = _pending_meta_block.data();
  for (auto block_index = 0u; block_index < num_blocks; ++block_index) {
    const auto out = _data.data() + _data_index;
    pack_block(in, out, bits_needed[block_index]);

    in += block_size;
    _data_index += bits_needed[block_index];
  }
}

void SimdBp128Encoder::pack_block(const uint32_t* _in, __m128i* out, const uint8_t bit_size) {
  auto in = reinterpret_cast<const __m128i*>(_in);

  auto in_reg = _mm_setzero_si128();
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1u << bit_size) - 1);

  switch (bit_size) {
    case 0u:
      Fail("Bit size of zero hasn’t been implemented.");
      return;

    case 1u:
      Pack128Bit<1u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 2u:
      Pack128Bit<2u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 3u:
      Pack128Bit<3u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 4u:
      Pack128Bit<4u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 5u:
      Pack128Bit<5u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 6u:
      Pack128Bit<6u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 7u:
      Pack128Bit<7u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 8u:
      Pack128Bit<8u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 9u:
      Pack128Bit<9u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 10u:
      Pack128Bit<10u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 11u:
      Pack128Bit<11u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 12u:
      Pack128Bit<12u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 13u:
      Pack128Bit<13u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 14u:
      Pack128Bit<14u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 15u:
      Pack128Bit<15u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 16u:
      Pack128Bit<16u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 17u:
      Pack128Bit<17u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 18u:
      Pack128Bit<18u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 19u:
      Pack128Bit<19u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 20u:
      Pack128Bit<20u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 21u:
      Pack128Bit<21u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 22u:
      Pack128Bit<22u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 23u:
      Pack128Bit<23u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 24u:
      Pack128Bit<24u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 25u:
      Pack128Bit<25u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 26u:
      Pack128Bit<26u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 27u:
      Pack128Bit<27u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 28u:
      Pack128Bit<28u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 29u:
      Pack128Bit<29u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 30u:
      Pack128Bit<30u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 31u:
      Pack128Bit<31u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 32u:
      Pack128Bit<32u>{}(in, out, in_reg, out_reg, mask);
      return;

    default:
      Fail("Bit size must be in range [0, 32]");
      return;
  }
}

}  // namespace opossum