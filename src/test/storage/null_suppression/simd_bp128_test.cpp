#include <memory>
#include <numeric>
#include <iostream>
#include <bitset>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/null_suppression/simd_bp128_encoder.hpp"
#include "storage/null_suppression/simd_bp128_vector.hpp"
#include "storage/null_suppression/simd_bp128_decoder.hpp"
#include "types.hpp"

namespace opossum {


class SimdBp128Test : public BaseTest {
 protected:
  void SetUp() override {}

  // Used for debugging purposes
  [[maybe_unused]] void print_encoded_vector(const SimdBp128Vector& vector) {
    for (auto _128_bit : vector.data()) {
      for (auto _32_bit : _128_bit.data) {
        std::cout << std::bitset<32>{_32_bit} << "|";
      }
      std::cout << std::endl;
    }
  }

  pmr_vector<uint32_t> generate_sequence(size_t count, uint32_t start, uint32_t increment) {
    auto sequence = pmr_vector<uint32_t>(count);
    auto value = start;
    for (auto& elem : sequence) {
      elem = value;
      value += increment;
    }

    return sequence;
  }

  std::unique_ptr<BaseNsVector> encode(const pmr_vector<uint32_t>& vector) {
    auto encoder = SimdBp128Encoder{};
    auto encoded_vector = encoder.encode(vector, vector.get_allocator());
    EXPECT_EQ(encoded_vector->size(), vector.size());

    return encoded_vector;
  }
};


TEST_F(SimdBp128Test, DecodeIncreasingSequenceUsingIterators) {
  const auto sequence = generate_sequence(4'200, 1'024, 8u);
  const auto encoded_sequence_base = encode(sequence);

  auto encoded_sequence = dynamic_cast<SimdBp128Vector*>(encoded_sequence_base.get());
  EXPECT_NE(encoded_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto encoded_seq_it = encoded_sequence->cbegin();
  for (; seq_it != seq_end; seq_it++, encoded_seq_it++) {
    EXPECT_EQ(*seq_it, *encoded_seq_it);
  }
}

TEST_F(SimdBp128Test, DecodeIncreasingSequenceUsingDecoder) {
  const auto sequence = generate_sequence(4'200, 1'024, 8u);
  const auto encoded_sequence = encode(sequence);

  auto decoder = encoded_sequence->create_base_decoder();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decoder->get(index));
  }
}

TEST_F(SimdBp128Test, DecodeSequenceOfZerosUsingIterators) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence_base = encode(sequence);

  auto encoded_sequence = dynamic_cast<SimdBp128Vector*>(encoded_sequence_base.get());
  EXPECT_NE(encoded_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto encoded_seq_it = encoded_sequence->cbegin();
  for (; seq_it != seq_end; seq_it++, encoded_seq_it++) {
    EXPECT_EQ(*seq_it, *encoded_seq_it);
  }
}

TEST_F(SimdBp128Test, DecodeSequenceOfZerosUsingDecoder) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence = encode(sequence);

  auto decoder = encoded_sequence->create_base_decoder();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decoder->get(index));
  }
}

}  // namespace opossum
