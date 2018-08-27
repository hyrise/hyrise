#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/dictionary_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

namespace opossum {

class StorageDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<int>> vc_int = std::make_shared<ValueSegment<int>>();
  std::shared_ptr<ValueSegment<std::string>> vc_str = std::make_shared<ValueSegment<std::string>>();
  std::shared_ptr<ValueSegment<double>> vc_double = std::make_shared<ValueSegment<double>>();
};

TEST_F(StorageDictionarySegmentTest, CompressColumnInt) {
  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(5);
  vc_int->append(3);

  auto col = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(col);

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

TEST_F(StorageDictionarySegmentTest, CompressColumnString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = encode_segment(EncodingType::Dictionary, DataType::String, vc_str);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<std::string>>(col);

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

TEST_F(StorageDictionarySegmentTest, CompressColumnDouble) {
  vc_double->append(0.9);
  vc_double->append(1.0);
  vc_double->append(1.0);
  vc_double->append(1.1);
  vc_double->append(0.9);
  vc_double->append(1.1);

  auto col = encode_segment(EncodingType::Dictionary, DataType::Double, vc_double);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<double>>(col);

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

TEST_F(StorageDictionarySegmentTest, CompressNullableColumnInt) {
  vc_int = std::make_shared<ValueSegment<int>>(true);

  vc_int->append(4);
  vc_int->append(4);
  vc_int->append(3);
  vc_int->append(4);
  vc_int->append(NULL_VALUE);
  vc_int->append(3);

  auto col = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 2u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], 3);
  EXPECT_EQ((*dict)[1], 4);

  // Test retrieval of null value
  EXPECT_TRUE(variant_is_null((*dict_segment)[4]));
}

TEST_F(StorageDictionarySegmentTest, LowerUpperBound) {
  for (int i = 0; i <= 10; i += 2) vc_int->append(i);

  auto col = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(col);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(4)), (ValueID)2);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(4)), (ValueID)3);

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(5)), (ValueID)3);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(5)), (ValueID)3);

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant(15)), INVALID_VALUE_ID);
}

TEST_F(StorageDictionarySegmentTest, FixedSizeByteAlignedVectorSize) {
  vc_int->append(0);
  vc_int->append(1);
  vc_int->append(2);

  auto col = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  auto dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(col);
  auto attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FixedSizeByteAlignedVector<uint8_t>>(dict_segment->attribute_vector());
  auto attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FixedSizeByteAlignedVector<uint16_t>>(dict_segment->attribute_vector());

  EXPECT_NE(attribute_vector_uint8_t, nullptr);
  EXPECT_EQ(attribute_vector_uint16_t, nullptr);

  for (int i = 3; i < 257; ++i) {
    vc_int->append(i);
  }

  col = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  dict_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(col);
  attribute_vector_uint8_t =
      std::dynamic_pointer_cast<const FixedSizeByteAlignedVector<uint8_t>>(dict_segment->attribute_vector());
  attribute_vector_uint16_t =
      std::dynamic_pointer_cast<const FixedSizeByteAlignedVector<uint16_t>>(dict_segment->attribute_vector());

  EXPECT_EQ(attribute_vector_uint8_t, nullptr);
  EXPECT_NE(attribute_vector_uint16_t, nullptr);
}

TEST_F(StorageDictionarySegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */

  const auto empty_memory_usage =
      encode_segment(EncodingType::Dictionary, DataType::Int, vc_int)->estimate_memory_usage();

  vc_int->append(0);
  vc_int->append(1);
  vc_int->append(2);
  const auto compressed_segment = encode_segment(EncodingType::Dictionary, DataType::Int, vc_int);
  const auto dictionary_segment = std::dynamic_pointer_cast<DictionarySegment<int>>(compressed_segment);

  static constexpr auto size_of_attribute = 1u;

  EXPECT_GE(dictionary_segment->estimate_memory_usage(), empty_memory_usage + 3 * size_of_attribute);
}

}  // namespace opossum
