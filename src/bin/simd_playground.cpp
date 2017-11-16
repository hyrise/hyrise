
#include <iostream>
#include <emmintrin.h>
#include <cstdint>
#include <bitset>

#include "storage/null_suppression/simd_bp128_encoder.hpp"
#include "storage/null_suppression/simd_bp128_vector.hpp"

void print_128_bit(__m128i reg) {
  __m128i var{};
  _mm_store_si128(&var, reg);

  auto _var = reinterpret_cast<uint32_t *>(&var);

  std::cout << std::bitset<32>{_var[3]} << "|";
  std::cout << std::bitset<32>{_var[2]} << "|";
  std::cout << std::bitset<32>{_var[1]} << "|";
  std::cout << std::bitset<32>{_var[0]} << std::endl << std::endl;
}

template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Pack128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;
    constexpr auto i_max = (_32_bit - carry_over) / bit_size;

    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = carry_over + i * bit_size;
      in_reg = _mm_and_si128(_mm_load_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset));
    }

    constexpr auto partial_fit_offset = carry_over + i_max * bit_size;
    constexpr auto num_first_bits = _32_bit - partial_fit_offset;

    if (partial_fit_offset < _32_bit) {
      in_reg = _mm_and_si128(_mm_load_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, partial_fit_offset));

      _mm_store_si128(out, out_reg);
      ++out;

      out_reg = _mm_srli_epi32(in_reg, num_first_bits);
    } else {
      _mm_store_si128(out, out_reg);
      ++out;
    }

    constexpr auto new_carry_over = bit_size - num_first_bits;
    Pack128Bit<bit_size, new_carry_over, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Pack128Bit<bit_size, carry_over, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

template <uint8_t bit_size, uint8_t carry_over = 0u, uint8_t remaining_recursions = bit_size>
struct Unpack128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;
    constexpr auto i_max = (_32_bit - carry_over) / bit_size;

    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = carry_over + i * bit_size;
      out_reg = _mm_and_si128(_mm_srli_epi32(in_reg, offset), mask);
      _mm_storeu_si128(out++, out_reg);
    }

    constexpr auto partial_fit_offset = carry_over + i_max * bit_size;
    constexpr auto num_first_bits = _32_bit - partial_fit_offset;

    if (partial_fit_offset < _32_bit) {
      out_reg = _mm_srli_epi32(in_reg, partial_fit_offset);
      in_reg = _mm_loadu_si128(in++);

      out_reg = _mm_or_si128(out_reg, _mm_and_si128(_mm_slli_epi32(in_reg, num_first_bits), mask));
      _mm_storeu_si128(out++, out_reg);
    } else {
      in_reg = _mm_loadu_si128(in++);
    }

    constexpr auto new_carry_over = bit_size - num_first_bits;
    Unpack128Bit<bit_size, new_carry_over, remaining_recursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t bit_size, uint8_t carry_over>
struct Unpack128Bit<bit_size, carry_over, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

template <uint8_t bit_size>
void pack_128(const uint32_t* _in, __m128i* out) {
  auto in = reinterpret_cast<const __m128i*>(_in);


  auto in_reg = _mm_setzero_si128();
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1u << bit_size) - 1);

  Pack128Bit<bit_size>{}(in, out, in_reg, out_reg, mask);
}

template <uint8_t bit_size>
void unpack_128(const __m128i* in, uint32_t* _out) {
  auto out = reinterpret_cast<__m128i*>(_out);

  auto in_reg = _mm_loadu_si128(in++);
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1u << bit_size) - 1);

  Unpack128Bit<bit_size>{}(in, out, in_reg, out_reg, mask);
}

int main(int argc, char const *argv[])
{
  auto encoder = opossum::SimdBp128Encoder{};

  encoder.init(136);

  for (auto i = 1024; i < 1160; ++i) {
    encoder.append(i);
  }

  encoder.finish();

  auto base_vector = std::shared_ptr<opossum::BaseEncodedVector>{encoder.get_vector()};
  auto vector = std::static_pointer_cast<opossum::SimdBp128Vector>(base_vector);

  auto data = vector->data();
  auto data_ptr = reinterpret_cast<const uint32_t *>(data.data());

  for (auto i = 0u; i < data.size(); ++i) {
    std::cout << std::bitset<32>{data_ptr[i * 4 + 3]} << "|";
    std::cout << std::bitset<32>{data_ptr[i * 4 + 2]} << "|";
    std::cout << std::bitset<32>{data_ptr[i * 4 + 1]} << "|";
    std::cout << std::bitset<32>{data_ptr[i * 4]} << std::endl;
  }

  return 0;
}