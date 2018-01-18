#include "simd_bp128_packing.hpp"

#include <emmintrin.h>

#include <algorithm>

#include "utils/assert.hpp"

namespace opossum {

namespace {

/**
 * @brief Packs 128 unsigned integers with the specified bit size
 *
 * Each recursion fills one 128-bit block. The number of 128-bit blocks needed to
 * pack 128 integers is equal to the bit size of each integer. The algorithm
 * packs the data interleaved, i.e., the first four integer are packed into
 * four different 32-bit words (within the 128-bit block). The fifth integer
 * directly succeeds the first and so on. If 32 is not a multiple of the bit size,
 * the last integers of each block are split and their ends stored in the
 * succeeding block. The following recursion needs to be aware of this and it is
 * represented by template parameter carry_over.
 *
 * @tparam bit_size number of bits each integer by which each integer is represented
 * @tparam carry_over the number of bits that have been carried over from the previous block
 */
template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Pack128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;

    // Number of integers that fit completely into the 32-bit sub-blocks
    constexpr auto i_max = (_32_bit - carry_over) / bit_size;

    // Fill the four 32-bit sub-blocks simultaneously by bit shifting
    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = carry_over + i * bit_size;
      in_reg = _mm_and_si128(_mm_loadu_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset));
    }

    constexpr auto next_offset = carry_over + i_max * bit_size;
    constexpr auto num_first_bits = _32_bit - next_offset;

    // Check if integers need to be split
    if (next_offset < _32_bit) {
      in_reg = _mm_and_si128(_mm_loadu_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, next_offset));

      _mm_store_si128(out, out_reg);
      ++out;

      out_reg = _mm_srli_epi32(in_reg, num_first_bits);
    } else {
      _mm_store_si128(out, out_reg);
      ++out;

      out_reg = _mm_setzero_si128();
    }

    // Calculate the new carry over
    constexpr auto new_carry_over = next_offset < _32_bit ? bit_size - num_first_bits : 0u;
    Pack128Bit<bit_size, new_carry_over, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Pack128Bit<bit_size, carry_over, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

/**
 * @brief Unpacks 128 unsigned integers with the specified bit size
 */
template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Unpack128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;

    // Number of integers that fit completely into the 32-bit sub-blocks
    constexpr auto i_max = (_32_bit - carry_over) / bit_size;

    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = carry_over + i * bit_size;
      out_reg = _mm_and_si128(_mm_srli_epi32(in_reg, offset), mask);
      _mm_storeu_si128(out++, out_reg);
    }

    constexpr auto next_offset = carry_over + i_max * bit_size;
    constexpr auto num_first_bits = _32_bit - next_offset;

    // Check if integers have been split across the 128-bit block boundary
    if (next_offset < _32_bit) {
      out_reg = _mm_srli_epi32(in_reg, next_offset);
      in_reg = _mm_load_si128(in++);

      out_reg = _mm_or_si128(out_reg, _mm_and_si128(_mm_slli_epi32(in_reg, num_first_bits), mask));
      _mm_storeu_si128(out++, out_reg);
    } else {
      constexpr auto last_recursion = 1u;

      // Only load another 128-bit block if it’s not the last recursion
      if (remaining_recursions > last_recursion) {
        in_reg = _mm_load_si128(in++);
      }
    }

    // Calculate the new carry over
    constexpr auto new_carry_over = next_offset < _32_bit ? bit_size - num_first_bits : 0u;
    Unpack128Bit<bit_size, new_carry_over, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Unpack128Bit<bit_size, carry_over, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

void unpack_128_zeros(uint32_t* out) {
  static constexpr auto num_zeros = 128u;
  std::fill(out, out + num_zeros, 0u);
}

}  // namespace

void SimdBp128Packing::write_meta_info(const uint8_t* _in, uint128_t* _out) {
  const auto in = reinterpret_cast<const __m128i*>(_in);
  auto out = reinterpret_cast<__m128i*>(_out);

  const auto meta_block_info_rgtr = _mm_loadu_si128(in);
  _mm_store_si128(out, meta_block_info_rgtr);
}

void SimdBp128Packing::read_meta_info(const uint128_t* _in, uint8_t* _out) {
  const auto in = reinterpret_cast<const __m128i*>(_in);
  auto out = reinterpret_cast<__m128i*>(_out);

  auto meta_info_block_rgtr = _mm_load_si128(in);
  _mm_storeu_si128(out, meta_info_block_rgtr);
}

void SimdBp128Packing::pack_block(const uint32_t* _in, uint128_t* _out, const uint8_t bit_size) {
  auto in = reinterpret_cast<const __m128i*>(_in);
  auto out = reinterpret_cast<__m128i*>(_out);

  auto in_reg = _mm_setzero_si128();
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1ul << bit_size) - 1);

  switch (bit_size) {
    case 0u:
      // No compression needed, since all values equal to zero.
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

void SimdBp128Packing::unpack_block(const uint128_t* _in, uint32_t* _out, const uint8_t bit_size) {
  if (bit_size == 0u) {
    unpack_128_zeros(_out);
    return;
  }

  auto in = reinterpret_cast<const __m128i*>(_in);
  auto out = reinterpret_cast<__m128i*>(_out);

  auto in_reg = _mm_load_si128(in++);
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1ul << bit_size) - 1);

  switch (bit_size) {
    case 1u:
      Unpack128Bit<1u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 2u:
      Unpack128Bit<2u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 3u:
      Unpack128Bit<3u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 4u:
      Unpack128Bit<4u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 5u:
      Unpack128Bit<5u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 6u:
      Unpack128Bit<6u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 7u:
      Unpack128Bit<7u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 8u:
      Unpack128Bit<8u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 9u:
      Unpack128Bit<9u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 10u:
      Unpack128Bit<10u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 11u:
      Unpack128Bit<11u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 12u:
      Unpack128Bit<12u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 13u:
      Unpack128Bit<13u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 14u:
      Unpack128Bit<14u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 15u:
      Unpack128Bit<15u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 16u:
      Unpack128Bit<16u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 17u:
      Unpack128Bit<17u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 18u:
      Unpack128Bit<18u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 19u:
      Unpack128Bit<19u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 20u:
      Unpack128Bit<20u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 21u:
      Unpack128Bit<21u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 22u:
      Unpack128Bit<22u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 23u:
      Unpack128Bit<23u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 24u:
      Unpack128Bit<24u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 25u:
      Unpack128Bit<25u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 26u:
      Unpack128Bit<26u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 27u:
      Unpack128Bit<27u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 28u:
      Unpack128Bit<28u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 29u:
      Unpack128Bit<29u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 30u:
      Unpack128Bit<30u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 31u:
      Unpack128Bit<31u>{}(in, out, in_reg, out_reg, mask);
      return;

    case 32u:
      Unpack128Bit<32u>{}(in, out, in_reg, out_reg, mask);
      return;

    default:
      Fail("Bit size must be in range [0, 32]");
      return;
  }
}

}  // namespace opossum
