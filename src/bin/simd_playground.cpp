
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

void pack_128(const uint32_t* _in, __m128i* out, const uint8_t bit_size) {
  constexpr auto _32_bit = 32;

  auto in = reinterpret_cast<const __m128i*>(_in);

  const auto mask = _mm_set1_epi32((1U << bit_size) - 1);

  auto in_reg = _mm_setzero_si128();
  auto out_reg = _mm_setzero_si128();

  auto offset = 0;
  for (auto _128_bit_block = 0; _128_bit_block < bit_size; ++_128_bit_block) {
    auto i_max = (_32_bit - offset) / bit_size;
    for (auto i = 0; i < i_max; ++i) {
      in_reg = _mm_and_si128(_mm_load_si128(in++), mask);
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset + i * bit_size));
    }

    if ((offset + i_max * bit_size) < _32_bit) {
      out_reg = _mm_or_si128(out_reg, _mm_slli_epi32(in_reg, offset + (i_max) * bit_size));
      _mm_store_si128(out, out_reg);
      ++out;

      auto carry_over = _32_bit - (offset + i_max * bit_size);
      out_reg = _mm_srli_epi32(in_reg, carry_over);

      offset = bit_size - carry_over;
    } else {
      _mm_store_si128(out, out_reg);
      ++out;
    }
  }
}

int main(int argc, char const *argv[])
{
  uint32_t in_array[128];

  for (auto i = 0; i < 128; ++i) {
    in_array[i] = 1;
  }

  __m128i out_array[5];

  pack_128(in_array, out_array, 5);

  auto _out_array = reinterpret_cast<uint32_t *>(out_array);

  for (auto i = 0; i < 5; ++i) {
    std::cout << std::bitset<32>{_out_array[i * 4 + 3]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4 + 2]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4 + 1]} << "|";
    std::cout << std::bitset<32>{_out_array[i * 4]} << std::endl;
  }

  return 0;
}