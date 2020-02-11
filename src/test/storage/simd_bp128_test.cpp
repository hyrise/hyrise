#include <bitset>
#include <iostream>
#include <memory>

#include <boost/hana/pair.hpp>

#include "base_test.hpp"

#include "storage/vector_compression/simd_bp128/simd_bp128_compressor.hpp"
#include "storage/vector_compression/simd_bp128/simd_bp128_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"
#include "types.hpp"

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

  pmr_vector<uint32_t> generate_sequence(const size_t count) {
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

auto simd_bp128_test_formatter = [](const ::testing::TestParamInfo<uint8_t> info) {
  return std::to_string(static_cast<uint32_t>(info.param));
};

INSTANTIATE_TEST_SUITE_P(BitSizes, SimdBp128Test, ::testing::Range(uint8_t{1}, uint8_t{33}), simd_bp128_test_formatter);

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

// Test that the iterator's advance() method works as expected. Creating a sufficiently large vector to ensure that
// accesses span multiple bounds. Note, as of February 2020, an issue with proxy iterators being only InputOperators
// leads to the problem that STL functionality (e.g., std::advance) does not use the iterator's advance() method to
// move n steps but rather calls increment()/decrement() n times (see issue #1531).
// Tests that advancing the iterators with step sizes >1 works for SIMD-BP128 vectors. Supplements CompressedVectorTest
// in that different bit sizes are tested.
TEST_P(SimdBp128Test, DecompressSequenceUsingAdvance) {
  const auto sequence = generate_sequence(SimdBp128Packing::meta_block_size * 2 + 17);
  const auto compressed_sequence_base = compress(sequence);
  auto compressed_sequence = dynamic_cast<const SimdBp128Vector*>(compressed_sequence_base.get());
  EXPECT_NE(compressed_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  auto compressed_seq_it = compressed_sequence->cbegin();
  EXPECT_EQ(*seq_it, *compressed_seq_it);

  seq_it += 1;
  compressed_seq_it += 1;
  EXPECT_EQ(*seq_it, *compressed_seq_it);

  seq_it += 5;
  compressed_seq_it += 5;
  EXPECT_EQ(*seq_it, *compressed_seq_it);

  seq_it += SimdBp128Packing::meta_block_size * 2;
  compressed_seq_it += SimdBp128Packing::meta_block_size * 2;
  EXPECT_EQ(*seq_it, *compressed_seq_it);

  auto seq_it_backwards = sequence.cend() - 1;  // last element
  auto compressed_seq_it_backwards = compressed_sequence->cend() - 1;
  EXPECT_EQ(*seq_it_backwards, *compressed_seq_it_backwards);
  seq_it -= 17;
  compressed_seq_it -= 17;
  EXPECT_EQ(*seq_it, *compressed_seq_it);

  seq_it -= SimdBp128Packing::meta_block_size + 17;
  compressed_seq_it -= SimdBp128Packing::meta_block_size + 17;
  EXPECT_EQ(*seq_it, *compressed_seq_it);
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

TEST_P(SimdBp128Test, CompressEmptySequence) {
  const auto sequence = generate_sequence(0);
  const auto compressed_sequence_base = compress(sequence);

  ASSERT_EQ(compressed_sequence_base->size(), 0u);
  ASSERT_EQ(compressed_sequence_base->data_size(), 0u);

  auto compressed_sequence = dynamic_cast<const SimdBp128Vector*>(compressed_sequence_base.get());
  EXPECT_NE(compressed_sequence, nullptr);

  auto decompressor = compressed_sequence->create_base_decompressor();
  ASSERT_EQ(decompressor->size(), 0u);
}

}  // namespace opossum
