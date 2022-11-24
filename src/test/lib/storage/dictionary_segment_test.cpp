#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"
#include "storage/vector_compression/vector_compression.hpp"

namespace hyrise {

class StorageDictionarySegmentTest : public BaseTestWithParam<VectorCompressionType> {
 protected:
  std::shared_ptr<ValueSegment<int>> vs_int = std::make_shared<ValueSegment<int>>();
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>();
  std::shared_ptr<ValueSegment<double>> vs_double = std::make_shared<ValueSegment<double>>();
};

auto dictionary_segment_test_formatter = [](const ::testing::TestParamInfo<VectorCompressionType> info) {
  const auto vector_compression = info.param;

  auto stream = std::stringstream{};
  stream << vector_compression;
  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(VectorCompressionTypes, StorageDictionarySegmentTest,
                         ::testing::Values(VectorCompressionType::FixedWidthInteger, VectorCompressionType::BitPacking),
                         dictionary_segment_test_formatter);

TEST_P(StorageDictionarySegmentTest, LowerUpperBound) {
  for (auto value = int32_t{0}; value <= 10; value += 2) {
    vs_int->append(value);
  }

  auto segment =
      ChunkEncoder::encode_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, GetParam()});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(4)), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(4)), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(5)), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(5)), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
}

TEST_P(StorageDictionarySegmentTest, CompressSegmentInt) {
  vs_int->append(4);
  vs_int->append(4);
  vs_int->append(3);
  vs_int->append(4);
  vs_int->append(5);
  vs_int->append(3);

  auto segment =
      ChunkEncoder::encode_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, GetParam()});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 3u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], 3);
  EXPECT_EQ((*dict)[1], 4);
  EXPECT_EQ((*dict)[2], 5);
}

TEST_P(StorageDictionarySegmentTest, CompressSegmentString) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Alexander");
  vs_str->append("Steve");
  vs_str->append("Hasso");
  vs_str->append("Bill");

  auto segment =
      ChunkEncoder::encode_segment(vs_str, DataType::String, SegmentEncodingSpec{EncodingType::Dictionary, GetParam()});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<pmr_string>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_P(StorageDictionarySegmentTest, CompressSegmentDouble) {
  vs_double->append(0.9);
  vs_double->append(1.0);
  vs_double->append(1.0);
  vs_double->append(1.1);
  vs_double->append(0.9);
  vs_double->append(1.1);

  auto segment = ChunkEncoder::encode_segment(vs_double, DataType::Double,
                                              SegmentEncodingSpec{EncodingType::Dictionary, GetParam()});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<double>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 3u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], 0.9);
  EXPECT_EQ((*dict)[1], 1.0);
  EXPECT_EQ((*dict)[2], 1.1);
}

TEST_P(StorageDictionarySegmentTest, CompressNullableSegmentInt) {
  vs_int = std::make_shared<ValueSegment<int>>(true);

  vs_int->append(4);
  vs_int->append(4);
  vs_int->append(3);
  vs_int->append(4);
  vs_int->append(NULL_VALUE);
  vs_int->append(3);

  auto segment =
      ChunkEncoder::encode_segment(vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, GetParam()});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 2u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], 3);
  EXPECT_EQ((*dict)[1], 4);

  // Test retrieval of null value
  EXPECT_TRUE(variant_is_null((*dict_segment)[ChunkOffset{4}]));
}

TEST_F(StorageDictionarySegmentTest, FixedWidthIntegerVectorSize) {
  vs_int->append(0);
  vs_int->append(1);
  vs_int->append(2);

  auto segment = ChunkEncoder::encode_segment(
      vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger});
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);
  auto attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint8_t>>(dict_segment->attribute_vector());
  auto attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint16_t>>(dict_segment->attribute_vector());

  EXPECT_NE(attribute_vector_uint8_t, nullptr);
  EXPECT_EQ(attribute_vector_uint16_t, nullptr);

  for (int i = 3; i < 257; ++i) {
    vs_int->append(i);
  }

  segment = ChunkEncoder::encode_segment(
      vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger});
  dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(segment);
  attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint8_t>>(dict_segment->attribute_vector());
  attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FixedWidthIntegerVector<uint16_t>>(dict_segment->attribute_vector());

  EXPECT_EQ(attribute_vector_uint8_t, nullptr);
  EXPECT_NE(attribute_vector_uint16_t, nullptr);
}

TEST_F(StorageDictionarySegmentTest, FixedWidthIntegerMemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  const auto empty_memory_usage =
      ChunkEncoder::encode_segment(
          vs_int, DataType::Int,
          SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger})
          ->memory_usage(MemoryUsageCalculationMode::Sampled);

  vs_int->append(0);
  vs_int->append(1);
  vs_int->append(2);
  auto compressed_segment = ChunkEncoder::encode_segment(
      vs_int, DataType::Int, SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedWidthInteger});
  const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(compressed_segment);

  static constexpr auto size_of_attribute = 1u;

  EXPECT_GE(dictionary_segment->memory_usage(MemoryUsageCalculationMode::Sampled),
            empty_memory_usage + 3 * size_of_attribute);
}

}  // namespace hyrise
