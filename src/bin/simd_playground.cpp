
#include <iostream>
#include <emmintrin.h>
#include <cstdint>
#include <bitset>

void print_128_bit(__m128i reg) {
  __m128i var{};
  _mm_store_si128(&var, reg);

  auto _var = reinterpret_cast<uint32_t *>(&var);

  std::cout << std::bitset<32>{_var[3]} << "|";
  std::cout << std::bitset<32>{_var[2]} << "|";
  std::cout << std::bitset<32>{_var[1]} << "|";
  std::cout << std::bitset<32>{_var[0]} << std::endl << std::endl;
}

template <uint8_t BitSize, uint8_t CarryOver = 0u, uint8_t RemainingRecursions = BitSize>
struct Fill128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;
    constexpr auto i_max = (_32_bit - CarryOver) / BitSize;

    for (auto i = 0u; i < i_max; ++i) {
      in_reg = _mm_and_si128(_mm_load_si128(in++), mask);
      const auto offset = CarryOver + i * BitSize;
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset));
    }

    constexpr auto partial_fit_offset = CarryOver + i_max * BitSize;
    constexpr auto num_first_bits = _32_bit - partial_fit_offset;

    if (partial_fit_offset < _32_bit) {
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, partial_fit_offset));

      _mm_store_si128(out, out_reg);
      ++out;

      out_reg = _mm_srli_epi32(in_reg, num_first_bits);
    } else {
      _mm_store_si128(out, out_reg);
      ++out;
    }

    constexpr auto new_carry_over = BitSize - num_first_bits;
    Fill128Bit<BitSize, new_carry_over, RemainingRecursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t BitSize, uint8_t CarryOver>
struct Fill128Bit<BitSize, CarryOver, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

template <uint8_t BitSize, uint8_t CarryOver = 0u, uint8_t RemainingRecursions = BitSize>
struct Load128Bit {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {
    constexpr auto _32_bit = 32u;
    constexpr auto i_max = (_32_bit - CarryOver) / BitSize;

    in_reg = _mm_loadu_si128(in++);

    for (auto i = 0u; i < i_max; ++i) {
      const auto offset = CarryOver + i * BitSize;
      out_reg = _mm_and_si128(_mm_srli_epi32(in_reg, offset), mask);
      _mm_storeu_si128(out++, out_reg);
    }

    constexpr auto partial_fit_offset = CarryOver + i_max * BitSize;
    constexpr auto num_first_bits = _32_bit - partial_fit_offset;

    if (partial_fit_offset < _32_bit) {
      out_reg = _mm_srli_epi32(in_reg, partial_fit_offset);

      in_reg = _mm_loadu_si128(in++);

      out_reg = _mm_or_si128(out_reg, _mm_and_si128(_mm_slli_epi32(in_reg, num_first_bits), mask));
      _mm_storeu_si128(out++, out_reg);
    } else {
      in_reg = _mm_loadu_si128(in++);
    }

    constexpr auto new_carry_over = BitSize - num_first_bits;
    Fill128Bit<BitSize, new_carry_over, RemainingRecursions - 1u>{}(in, out, in_reg, out_reg, mask);
  }
};

template <uint8_t BitSize, uint8_t CarryOver>
struct Load128Bit<BitSize, CarryOver, 0u> {
  void operator()(const __m128i* in, __m128i* out, __m128i& in_reg, __m128i& out_reg, const __m128i& mask) const {}
};

template <uint8_t BitSize>
void pack_128(const uint32_t* _in, __m128i* out) {
  auto in = reinterpret_cast<const __m128i*>(_in);


  auto in_reg = _mm_setzero_si128();
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1u << BitSize) - 1);

  Fill128Bit<BitSize>{}(in, out, in_reg, out_reg, mask);
}

template <uint8_t BitSize>
void unpack_128(const __m128i* in, uint32_t* _out) {
  auto out = reinterpret_cast<const __m128i*>(_out);

  auto in_reg = _mm_loadu_si128(in++);
  auto out_reg = _mm_setzero_si128();
  const auto mask = _mm_set1_epi32((1u << BitSize) - 1);

  Load128Bit<BitSize>{}(in, out, in_reg, out_reg, mask);
}

int main(int argc, char const *argv[])
{
  uint32_t in_array[128];

  for (auto i = 0; i < 128; ++i) {
    in_array[i] = 1;
  }

  constexpr auto bit_size = 5u;

  __m128i out_array[bit_size];

  pack_128<bit_size>(in_array, out_array);

  auto _out_array = reinterpret_cast<uint32_t *>(out_array);

  for (auto i = 0u; i < bit_size; ++i) {
    std::cout << std::bitset<32>{_out_array[i * 4 + 3]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4 + 2]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4 + 1]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4]} << std::endl;
  }

  return 0;
}