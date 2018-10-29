#include <bitset>
#include <iostream>
#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/vector_compression/resolve_compressed_vector_type.hpp"
#include "storage/vector_compression/vector_compression.hpp"

#include "constant_mappings.hpp"
#include "types.hpp"

namespace opossum {

namespace {

// Used for debugging purposes
[[maybe_unused]] void print_encoded_vector(const SimdBp128Vector& vector) {
  for (auto _128_bit : vector.data()) {
    for (auto _32_bit : _128_bit.data) {
      std::cout << std::bitset<32>{_32_bit} << "|";
    }
    std::cout << std::endl;
  }
}

}  // namespace

class CompressedVectorTest : public BaseTestWithParam<VectorCompressionType> {
 protected:
  void SetUp() override {}

  auto min() { return 1'024; }

  auto max() { return 34'624; }

  pmr_vector<uint32_t> generate_sequence(size_t count, uint32_t increment) {
    auto sequence = pmr_vector<uint32_t>(count);
    auto value = min();
    for (auto& elem : sequence) {
      elem = value;

      value += increment;
      if (value > max()) value = min();
    }

    return sequence;
  }

  std::unique_ptr<const BaseCompressedVector> encode(const pmr_vector<uint32_t>& vector) {
    const auto compression_type = GetParam();

    auto encoded_vector = compress_vector(vector, compression_type, {}, {max()});
    EXPECT_EQ(encoded_vector->size(), vector.size());

    return encoded_vector;
  }

  template <typename ZeroSuppressionVectorT>
  void compare_using_iterator(const ZeroSuppressionVectorT& encoded_sequence,
                              const pmr_vector<uint32_t>& expected_values) {
    auto expected_it = expected_values.cbegin();
    auto encoded_seq_it = encoded_sequence.cbegin();
    const auto encoded_seq_end = encoded_sequence.cend();
    for (; encoded_seq_it != encoded_seq_end; expected_it++, encoded_seq_it++) {
      EXPECT_EQ(*encoded_seq_it, *expected_it);
    }
  }
};

auto formatter = [](const ::testing::TestParamInfo<VectorCompressionType> info) {
  const auto type = info.param;
  auto string = vector_compression_type_to_string.left.at(type);
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());
  return string;
};

INSTANTIATE_TEST_CASE_P(VectorCompressionTypes, CompressedVectorTest,
                        ::testing::Values(VectorCompressionType::SimdBp128,
                                          VectorCompressionType::FixedSizeByteAligned),
                        formatter);

TEST_P(CompressedVectorTest, DecodeIncreasingSequenceUsingIterators) {
  const auto sequence = this->generate_sequence(4'200, 8u);
  const auto encoded_sequence_base = this->encode(sequence);

  resolve_compressed_vector_type(*encoded_sequence_base,
                                 [&](auto& encoded_sequence) { compare_using_iterator(encoded_sequence, sequence); });
}

TEST_P(CompressedVectorTest, DecodeIncreasingSequenceUsingDecompressor) {
  const auto sequence = this->generate_sequence(4'200, 8u);
  const auto encoded_sequence = this->encode(sequence);

  auto decompressor = encoded_sequence->create_base_decompressor();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decompressor->get(index));
  }
}

TEST_P(CompressedVectorTest, DecodeSequenceOfZerosUsingIterators) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence_base = this->encode(sequence);

  resolve_compressed_vector_type(*encoded_sequence_base,
                                 [&](auto& encoded_sequence) { compare_using_iterator(encoded_sequence, sequence); });
}

TEST_P(CompressedVectorTest, DecodeSequenceOfZerosUsingDecompressor) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence = this->encode(sequence);

  auto decompressor = encoded_sequence->create_base_decompressor();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decompressor->get(index));
  }
}

}  // namespace opossum
