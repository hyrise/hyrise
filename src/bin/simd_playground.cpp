
#include <iostream>
#include <emmintrin.h>
#include <cstdint>
#include <bitset>

#include "storage/null_suppression/simd_bp128_decoder.hpp"
#include "storage/null_suppression/simd_bp128_encoder.hpp"
#include "storage/null_suppression/simd_bp128_vector.hpp"

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

  auto decoder = opossum::SimdBp128Decoder{*vector};

  auto decoded_vector = decoder.decode();

  std::cout << std::endl;
  for (auto var : decoded_vector) {
    std::cout << var << std::endl;
  }

  return 0;
}
