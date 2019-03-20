#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/fixed_string_dictionary_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

namespace opossum {

class StorageFixedStringDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>();
};

TEST_F(StorageFixedStringDictionarySegmentTest, CompressSegmentString) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Alexander");
  vs_str->append("Steve");
  vs_str->append("Hasso");
  vs_str->append("Bill");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);
  EXPECT_EQ(dict_segment->attribute_vector()->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageFixedStringDictionarySegmentTest, Decode) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Bill");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  EXPECT_EQ(dict_segment->encoding_type(), EncodingType::FixedStringDictionary);
  EXPECT_EQ(dict_segment->compressed_vector_type(), CompressedVectorType::FixedSize1ByteAligned);

  // Decode values
  EXPECT_EQ((*dict_segment)[0], AllTypeVariant("Bill"));
  EXPECT_EQ((*dict_segment)[1], AllTypeVariant("Steve"));
  EXPECT_EQ((*dict_segment)[2], AllTypeVariant("Bill"));
}

TEST_F(StorageFixedStringDictionarySegmentTest, LongStrings) {
  vs_str->append("ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
  vs_str->append("QuiteShort");
  vs_str->append("Short");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  // Test sorting
  auto dict = dict_segment->dictionary();
  EXPECT_EQ((*dict)[0], "QuiteShort");
  EXPECT_EQ((*dict)[1], "Short");
  EXPECT_EQ((*dict)[2], "ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
}

TEST_F(StorageFixedStringDictionarySegmentTest, CopyUsingAlloctor) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Alexander");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  auto alloc = dict_segment->dictionary()->get_allocator();
  auto base_segment = dict_segment->copy_using_allocator(alloc);
  auto dict_segment_copy = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(base_segment);

  EXPECT_EQ(dict_segment->dictionary()->get_allocator(), dict_segment_copy->dictionary()->get_allocator());
  auto dict = dict_segment_copy->dictionary();

  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Steve");
}

TEST_F(StorageFixedStringDictionarySegmentTest, LowerUpperBound) {
  vs_str->append("A");
  vs_str->append("C");
  vs_str->append("E");
  vs_str->append("G");
  vs_str->append("I");
  vs_str->append("K");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("E")), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("E")), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("F")), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("F")), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
}

TEST_F(StorageFixedStringDictionarySegmentTest, NullValues) {
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);

  vs_str->append("A");
  vs_str->append(NULL_VALUE);
  vs_str->append("E");

  auto segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  auto dict_segment = std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(segment);

  EXPECT_EQ(dict_segment->null_value_id(), 2u);
  EXPECT_TRUE(variant_is_null((*dict_segment)[1]));
}

TEST_F(StorageFixedStringDictionarySegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */
  const auto empty_compressed_segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  const auto empty_dictionary_segment =
      std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(empty_compressed_segment);
  const auto empty_memory_usage = empty_dictionary_segment->estimate_memory_usage();

  vs_str->append("A");
  vs_str->append("B");
  vs_str->append("C");
  const auto compressed_segment = encode_segment(EncodingType::FixedStringDictionary, DataType::String, vs_str);
  const auto dictionary_segment =
      std::dynamic_pointer_cast<FixedStringDictionarySegment<pmr_string>>(compressed_segment);

  static constexpr auto size_of_attribute = 1u;
  static constexpr auto size_of_dictionary = 3u;

  // We have to substract 1 since the empty FixedStringSegment actually contains one null terminator
  EXPECT_EQ(dictionary_segment->estimate_memory_usage(),
            empty_memory_usage - 1u + 3 * size_of_attribute + size_of_dictionary);
}

}  // namespace opossum
