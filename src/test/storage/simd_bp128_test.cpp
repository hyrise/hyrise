#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>

#include <bitset>
#include <iostream>
#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/vector_compression/simd_bp128/simd_bp128_compressor.hpp"
#include "storage/vector_compression/simd_bp128/simd_bp128_decompressor.hpp"
#include "storage/vector_compression/simd_bp128/simd_bp128_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"

#include "types.hpp"
#include "utils/enum_constant.hpp"

namespace opossum {

namespace {

// Used for debugging purposes
[[maybe_unused]] void print_compressed_vector(const SimdBp128Vector& vector) {
  for (auto _128_bit : vector.data()) {
    for (auto _32_bit : _128_bit.data) {
      std::cout << std::bitset<32>{_32_bit} << "|";
    }
    std::cout << std::endl;
  }
}

}  // namespace

class SimdBp128Test : public BaseTest, public ::testing::WithParamInterface<uint8_t> {
 protected:
  void SetUp() override {
    _bit_size = GetParam();
    _min = static_cast<uint32_t>(1ul << (_bit_size - 1u));
    _max = static_cast<uint32_t>((1ul << _bit_size) - 1u);
  }

  pmr_vector<uint32_t> generate_sequence(size_t count) {
    auto sequence = pmr_vector<uint32_t>(count);
    auto value = _min;
    for (auto& elem : sequence) {
      elem = value;

      value += 1u;
      if (value > _max) value = _min;
    }

    return sequence;
  }

  std::unique_ptr<const BaseCompressedVector> compress(const pmr_vector<uint32_t>& vector) {
    auto compressor = SimdBp128Compressor{};
    auto compressed_vector = compressor.compress(vector, vector.get_allocator());
    EXPECT_EQ(compressed_vector->size(), vector.size());

    return compressed_vector;
  }

 private:
  uint8_t _bit_size;
  uint32_t _min;
  uint32_t _max;
};

auto formatter = [](const ::testing::TestParamInfo<uint8_t> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

INSTANTIATE_TEST_CASE_P(BitSizes, SimdBp128Test, ::testing::Range(uint8_t{1}, uint8_t{33}), formatter);

TEST_P(SimdBp128Test, DecompressSequenceUsingIterators) {
  const auto sequence = generate_sequence(420);
  const auto compressed_sequence_base = compress(sequence);

  auto compressed_sequence = dynamic_cast<const SimdBp128Vector*>(compressed_sequence_base.get());
  EXPECT_NE(compressed_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  auto compressed_seq_it = compressed_sequence->cbegin();
  const auto compressed_seq_end = compressed_sequence->cend();
  for (; compressed_seq_it != compressed_seq_end; seq_it++, compressed_seq_it++) {
    EXPECT_EQ(*seq_it, *compressed_seq_it);
  }
}

TEST_P(SimdBp128Test, DecompressSequenceUsingDecompressor) {
  const auto sequence = generate_sequence(420);
  const auto compressed_sequence = compress(sequence);

  auto decompressor = compressed_sequence->create_base_decompressor();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decompressor->get(index));
  }
}

}  // namespace opossum
