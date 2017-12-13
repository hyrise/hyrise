#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>

#include <bitset>
#include <iostream>
#include <memory>
#include <numeric>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/zero_suppression/decoders.hpp"
#include "storage/zero_suppression/encoders.hpp"
#include "storage/zero_suppression/vectors.hpp"
#include "storage/zero_suppression/utils.hpp"

#include "types.hpp"
#include "utils/enum_constant.hpp"

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

namespace hana = boost::hana;

using ZsTypes =
    ::testing::Types<enum_constant<ZsType, ZsType::SimdBp128>, enum_constant<ZsType, ZsType::FixedSize4ByteAligned>,
                     enum_constant<ZsType, ZsType::FixedSize2ByteAligned>,
                     enum_constant<ZsType, ZsType::FixedSize1ByteAligned>>;

constexpr auto range_for_zs_type =
    hana::make_map(hana::make_pair(enum_c<ZsType, ZsType::SimdBp128>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<ZsType, ZsType::FixedSize4ByteAligned>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<ZsType, ZsType::FixedSize2ByteAligned>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<ZsType, ZsType::FixedSize1ByteAligned>, hana::make_pair(0, 255)));

template <typename ZsTypeT>
class ZeroSuppressionTest : public BaseTest {
 protected:
  static constexpr auto vector_t = hana::at_key(zs_vector_for_type, ZsTypeT{});
  static constexpr auto encoder_t = hana::at_key(zs_encoder_for_type, ZsTypeT{});

  using VectorType = typename decltype(vector_t)::type;
  using EncoderType = typename decltype(encoder_t)::type;

 protected:
  void SetUp() override {}

  auto min() { return hana::first(hana::at_key(range_for_zs_type, ZsTypeT{})); }

  auto max() { return hana::second(hana::at_key(range_for_zs_type, ZsTypeT{})); }

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

  std::unique_ptr<BaseZeroSuppressionVector> encode(const pmr_vector<uint32_t>& vector) {
    auto encoder = EncoderType{};
    auto encoded_vector = encoder.encode(vector, vector.get_allocator());
    EXPECT_EQ(encoded_vector->size(), vector.size());

    return encoded_vector;
  }
};

TYPED_TEST_CASE(ZeroSuppressionTest, ZsTypes);

TYPED_TEST(ZeroSuppressionTest, DecodeIncreasingSequenceUsingIterators) {
  const auto sequence = this->generate_sequence(4'200, 8u);
  const auto encoded_sequence_base = this->encode(sequence);

  auto encoded_sequence = dynamic_cast<typename TestFixture::VectorType*>(encoded_sequence_base.get());
  EXPECT_NE(encoded_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  auto encoded_seq_it = encoded_sequence->cbegin();
  const auto encoded_seq_end = encoded_sequence->cend();
  for (; encoded_seq_it != encoded_seq_end; seq_it++, encoded_seq_it++) {
    EXPECT_EQ(*seq_it, *encoded_seq_it);
  }
}

TYPED_TEST(ZeroSuppressionTest, DecodeIncreasingSequenceUsingDecoder) {
  const auto sequence = this->generate_sequence(4'200, 8u);
  const auto encoded_sequence = this->encode(sequence);

  auto decoder = encoded_sequence->create_base_decoder();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decoder->get(index));
  }
}

TYPED_TEST(ZeroSuppressionTest, DecodeSequenceOfZerosUsingIterators) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence_base = this->encode(sequence);

  auto encoded_sequence = dynamic_cast<typename TestFixture::VectorType*>(encoded_sequence_base.get());
  EXPECT_NE(encoded_sequence, nullptr);

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto encoded_seq_it = encoded_sequence->cbegin();
  for (; seq_it != seq_end; seq_it++, encoded_seq_it++) {
    EXPECT_EQ(*seq_it, *encoded_seq_it);
  }
}

TYPED_TEST(ZeroSuppressionTest, DecodeSequenceOfZerosUsingDecoder) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence = this->encode(sequence);

  auto decoder = encoded_sequence->create_base_decoder();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto index = 0u;
  for (; seq_it != seq_end; seq_it++, index++) {
    EXPECT_EQ(*seq_it, decoder->get(index));
  }
}

TYPED_TEST(ZeroSuppressionTest, DecodeSequenceOfZerosUsingDecodeMethod) {
  const auto sequence = pmr_vector<uint32_t>(2'200, 0u);
  const auto encoded_sequence = this->encode(sequence);

  auto decoded_sequence = encoded_sequence->decode();

  auto seq_it = sequence.cbegin();
  const auto seq_end = sequence.cend();
  auto decoded_seq_it = decoded_sequence.cbegin();
  for (; seq_it != seq_end; seq_it++, decoded_seq_it++) {
    EXPECT_EQ(*seq_it, *decoded_seq_it);
  }
}

}  // namespace opossum
