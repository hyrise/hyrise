#include <boost/hana/at_key.hpp>
#include <boost/hana/map.hpp>
#include <boost/hana/pair.hpp>

#include <bitset>
#include <iostream>
#include <memory>
#include <numeric>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/null_suppression/ns_decoders.hpp"
#include "storage/null_suppression/ns_encoders.hpp"
#include "storage/null_suppression/ns_utils.hpp"
#include "storage/null_suppression/ns_vectors.hpp"

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

using NsTypes =
    ::testing::Types<enum_constant<NsType, NsType::SimdBp128>, enum_constant<NsType, NsType::FixedSize4ByteAligned>,
                     enum_constant<NsType, NsType::FixedSize2ByteAligned>,
                     enum_constant<NsType, NsType::FixedSize1ByteAligned>>;

constexpr auto range_for_ns_type =
    hana::make_map(hana::make_pair(enum_c<NsType, NsType::SimdBp128>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<NsType, NsType::FixedSize4ByteAligned>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<NsType, NsType::FixedSize2ByteAligned>, hana::make_pair(1'024, 34'624)),
                   hana::make_pair(enum_c<NsType, NsType::FixedSize1ByteAligned>, hana::make_pair(0, 255)));

template <typename NsTypeT>
class NullSuppressionTest : public BaseTest {
 protected:
  static constexpr auto vector_t = hana::at_key(ns_vector_for_type, NsTypeT{});
  static constexpr auto encoder_t = hana::at_key(ns_encoder_for_type, NsTypeT{});

  using VectorType = typename decltype(vector_t)::type;
  using EncoderType = typename decltype(encoder_t)::type;

 protected:
  void SetUp() override {}

  auto min() { return hana::first(hana::at_key(range_for_ns_type, NsTypeT{})); }

  auto max() { return hana::second(hana::at_key(range_for_ns_type, NsTypeT{})); }

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

  std::unique_ptr<BaseNsVector> encode(const pmr_vector<uint32_t>& vector) {
    auto encoder = EncoderType{};
    auto encoded_vector = encoder.encode(vector, vector.get_allocator());
    EXPECT_EQ(encoded_vector->size(), vector.size());

    return encoded_vector;
  }
};

TYPED_TEST_CASE(NullSuppressionTest, NsTypes);

TYPED_TEST(NullSuppressionTest, DecodeIncreasingSequenceUsingIterators) {
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

TYPED_TEST(NullSuppressionTest, DecodeIncreasingSequenceUsingDecoder) {
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

TYPED_TEST(NullSuppressionTest, DecodeSequenceOfZerosUsingIterators) {
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

TYPED_TEST(NullSuppressionTest, DecodeSequenceOfZerosUsingDecoder) {
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

TYPED_TEST(NullSuppressionTest, DecodeSequenceOfZerosUsingDecodeMethod) {
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
