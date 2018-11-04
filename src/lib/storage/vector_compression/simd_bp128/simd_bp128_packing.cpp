#include "simd_bp128_packing.hpp"

#include <algorithm>

#include "utils/assert.hpp"

// When casting into this data type, make sure that the underlying data is properly aligned to 16 byte boundaries.
using simd_type = uint32_t __attribute__((vector_size(16)));

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
  void operator()(const simd_type* in, simd_type* out, simd_type& in_reg, simd_type& out_reg,
                  const simd_type& mask) const {
    constexpr auto BITS_IN_WORD = 32u;

    // Number of integers that fit completely into the 32-bit sub-blocks
    constexpr auto I_MAX = (BITS_IN_WORD - carry_over) / bit_size;

    // Fill the four 32-bit sub-blocks simultaneously by bit shifting
    for (auto i = 0u; i < I_MAX; ++i) {
      const auto offset = carry_over + i * bit_size;
      in_reg = *in++ & mask;
      out_reg = out_reg | (in_reg << offset);
    }

    constexpr auto NEXT_OFFSET = carry_over + I_MAX * bit_size;
    constexpr auto NUM_FIRST_BITS = BITS_IN_WORD - NEXT_OFFSET;

    // Check if integers need to be split
    if (NEXT_OFFSET < BITS_IN_WORD) {
      in_reg = *in++ & mask;
      out_reg = out_reg | (in_reg << NEXT_OFFSET);

      *out = out_reg;
      ++out;

      out_reg = in_reg >> NUM_FIRST_BITS;
    } else {
      *out = out_reg;
      out++;

      simd_type zero_reg = {0, 0, 0, 0};
      out_reg = zero_reg;
    }

    // Calculate the new carry over
    constexpr auto NEW_CARRY_OVER = NEXT_OFFSET < BITS_IN_WORD ? bit_size - NUM_FIRST_BITS : 0u;
    Pack128Bit<bit_size, NEW_CARRY_OVER, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Pack128Bit<bit_size, carry_over, 0u> {
  void operator()(const simd_type* in, simd_type* out, simd_type& in_reg, simd_type& out_reg,
                  const simd_type& mask) const {}
};

/**
 * @brief Unpacks 128 unsigned integers with the specified bit size
 */
template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Unpack128Bit {
  void operator()(const simd_type* in, simd_type* out, simd_type& in_reg, simd_type& out_reg,
                  const simd_type& mask) const {
    constexpr auto BITS_IN_WORD = 32u;

    // Number of integers that fit completely into the 32-bit sub-blocks
    constexpr auto I_MAX = (BITS_IN_WORD - carry_over) / bit_size;

    for (auto i = 0u; i < I_MAX; ++i) {
      const auto offset = carry_over + i * bit_size;
      out_reg = (in_reg >> offset) & mask;
      *out++ = out_reg;
    }

    constexpr auto NEXT_OFFSET = carry_over + I_MAX * bit_size;
    constexpr auto NUM_FIRST_BITS = BITS_IN_WORD - NEXT_OFFSET;

    // Check if integers have been split across the 128-bit block boundary
    if (NEXT_OFFSET < BITS_IN_WORD) {
      out_reg = in_reg >> NEXT_OFFSET;
      in_reg = *in++;

      out_reg = out_reg | ((in_reg << NUM_FIRST_BITS) & mask);
      *out++ = out_reg;
    } else {
      constexpr auto LAST_RECURSION = 1u;

      // Only load another 128-bit block if it’s not the last recursion
      if (remaining_recursions > LAST_RECURSION) {
        in_reg = *in++;
      }
    }

    // Calculate the new carry over
    constexpr auto NEW_CARRY_OVER = NEXT_OFFSET < BITS_IN_WORD ? bit_size - NUM_FIRST_BITS : 0u;
    Unpack128Bit<bit_size, NEW_CARRY_OVER, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Unpack128Bit<bit_size, carry_over, 0u> {
  void operator()(const simd_type* in, simd_type* out, simd_type& in_reg, simd_type& out_reg,
                  const simd_type& mask) const {}
};

void unpack_128_zeros(uint32_t* out) {
  static constexpr auto NUM_ZEROES = 128u;
  std::fill(out, out + NUM_ZEROES, 0u);
}

}  // namespace

void SimdBp128Packing::write_meta_info(const uint8_t* in, uint128_t* out) {
  const auto simd_in = reinterpret_cast<const simd_type*>(in);
  auto simd_out = reinterpret_cast<simd_type*>(out);

  *simd_out = *simd_in;
}

void SimdBp128Packing::read_meta_info(const uint128_t* in, uint8_t* out) {
  const auto simd_in = reinterpret_cast<const simd_type*>(in);
  auto simd_out = reinterpret_cast<simd_type*>(out);

  *simd_out = *simd_in;
}

void SimdBp128Packing::pack_block(const uint32_t* in, uint128_t* out, const uint8_t bit_size) {
  auto simd_in = reinterpret_cast<const simd_type*>(in);
  auto simd_out = reinterpret_cast<simd_type*>(out);

  simd_type in_reg = {0, 0, 0, 0};
  simd_type out_reg = {0, 0, 0, 0};
  unsigned int one_mask = (1ul << bit_size) - 1;
  const simd_type mask = {one_mask, one_mask, one_mask, one_mask};

  switch (bit_size) {
    case 0u:
      // No compression needed, since all values equal to zero.
      return;

    case 1u:
      Pack128Bit<1u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 2u:
      Pack128Bit<2u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 3u:
      Pack128Bit<3u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 4u:
      Pack128Bit<4u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 5u:
      Pack128Bit<5u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 6u:
      Pack128Bit<6u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 7u:
      Pack128Bit<7u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 8u:
      Pack128Bit<8u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 9u:
      Pack128Bit<9u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 10u:
      Pack128Bit<10u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 11u:
      Pack128Bit<11u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 12u:
      Pack128Bit<12u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 13u:
      Pack128Bit<13u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 14u:
      Pack128Bit<14u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 15u:
      Pack128Bit<15u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 16u:
      Pack128Bit<16u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 17u:
      Pack128Bit<17u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 18u:
      Pack128Bit<18u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 19u:
      Pack128Bit<19u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 20u:
      Pack128Bit<20u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 21u:
      Pack128Bit<21u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 22u:
      Pack128Bit<22u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 23u:
      Pack128Bit<23u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 24u:
      Pack128Bit<24u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 25u:
      Pack128Bit<25u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 26u:
      Pack128Bit<26u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 27u:
      Pack128Bit<27u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 28u:
      Pack128Bit<28u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 29u:
      Pack128Bit<29u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 30u:
      Pack128Bit<30u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 31u:
      Pack128Bit<31u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 32u:
      Pack128Bit<32u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    default:
      Fail("Bit size must be in range [0, 32]");
      return;
  }
}

void SimdBp128Packing::unpack_block(const uint128_t* in, uint32_t* out, const uint8_t bit_size) {
  if (bit_size == 0u) {
    unpack_128_zeros(out);
    return;
  }

  auto simd_in = reinterpret_cast<const simd_type*>(in);
  auto simd_out = reinterpret_cast<simd_type*>(out);

  simd_type in_reg = *simd_in++;
  simd_type out_reg = {0, 0, 0, 0};
  unsigned int one_mask = (1ul << bit_size) - 1;
  const simd_type mask = {one_mask, one_mask, one_mask, one_mask};

  switch (bit_size) {
    case 1u:
      Unpack128Bit<1u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 2u:
      Unpack128Bit<2u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 3u:
      Unpack128Bit<3u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 4u:
      Unpack128Bit<4u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 5u:
      Unpack128Bit<5u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 6u:
      Unpack128Bit<6u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 7u:
      Unpack128Bit<7u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 8u:
      Unpack128Bit<8u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 9u:
      Unpack128Bit<9u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 10u:
      Unpack128Bit<10u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 11u:
      Unpack128Bit<11u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 12u:
      Unpack128Bit<12u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 13u:
      Unpack128Bit<13u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 14u:
      Unpack128Bit<14u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 15u:
      Unpack128Bit<15u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 16u:
      Unpack128Bit<16u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 17u:
      Unpack128Bit<17u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 18u:
      Unpack128Bit<18u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 19u:
      Unpack128Bit<19u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 20u:
      Unpack128Bit<20u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 21u:
      Unpack128Bit<21u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 22u:
      Unpack128Bit<22u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 23u:
      Unpack128Bit<23u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 24u:
      Unpack128Bit<24u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 25u:
      Unpack128Bit<25u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 26u:
      Unpack128Bit<26u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 27u:
      Unpack128Bit<27u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 28u:
      Unpack128Bit<28u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 29u:
      Unpack128Bit<29u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 30u:
      Unpack128Bit<30u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 31u:
      Unpack128Bit<31u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    case 32u:
      Unpack128Bit<32u>{}(simd_in, simd_out, in_reg, out_reg, mask);
      return;

    default:
      Fail("Bit size must be in range [0, 32]");
      return;
  }
}

}  // namespace opossum
