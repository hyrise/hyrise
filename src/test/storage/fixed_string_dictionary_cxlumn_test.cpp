#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/column_encoding_utils.hpp"
#include "storage/fixed_string_dictionary_column.hpp"
#include "storage/value_segment.hpp"
#include "storage/vector_compression/fixed_size_byte_aligned/fixed_size_byte_aligned_vector.hpp"

namespace opossum {

class StorageFixedStringDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<std::string>> vc_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(StorageFixedStringDictionarySegmentTest, CompressColumnString) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");
  vc_str->append("Steve");
  vc_str->append("Hasso");
  vc_str->append("Bill");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  // Test attribute_vector size
  EXPECT_EQ(dict_col->size(), 6u);
  EXPECT_EQ(dict_col->attribute_vector()->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_col->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Hasso");
  EXPECT_EQ((*dict)[3], "Steve");
}

TEST_F(StorageFixedStringDictionarySegmentTest, Decode) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Bill");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  EXPECT_EQ(dict_col->encoding_type(), EncodingType::FixedStringDictionary);
  EXPECT_EQ(dict_col->compressed_vector_type(), CompressedVectorType::FixedSize1ByteAligned);

  // Decode values
  EXPECT_EQ((*dict_col)[0], AllTypeVariant("Bill"));
  EXPECT_EQ((*dict_col)[1], AllTypeVariant("Steve"));
  EXPECT_EQ((*dict_col)[2], AllTypeVariant("Bill"));
}

TEST_F(StorageFixedStringDictionarySegmentTest, LongStrings) {
  vc_str->append("ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
  vc_str->append("QuiteShort");
  vc_str->append("Short");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  // Test sorting
  auto dict = dict_col->dictionary();
  EXPECT_EQ((*dict)[0], "QuiteShort");
  EXPECT_EQ((*dict)[1], "Short");
  EXPECT_EQ((*dict)[2], "ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
}

TEST_F(StorageFixedStringDictionarySegmentTest, CopyUsingAlloctor) {
  vc_str->append("Bill");
  vc_str->append("Steve");
  vc_str->append("Alexander");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  auto alloc = dict_col->dictionary()->get_allocator();
  auto base_column = dict_col->copy_using_allocator(alloc);
  auto dict_col_copy = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(base_column);

  EXPECT_EQ(dict_col->dictionary()->get_allocator(), dict_col_copy->dictionary()->get_allocator());
  auto dict = dict_col_copy->dictionary();

  EXPECT_EQ((*dict)[0], "Alexander");
  EXPECT_EQ((*dict)[1], "Bill");
  EXPECT_EQ((*dict)[2], "Steve");
}

TEST_F(StorageFixedStringDictionarySegmentTest, LowerUpperBound) {
  vc_str->append("A");
  vc_str->append("C");
  vc_str->append("E");
  vc_str->append("G");
  vc_str->append("I");
  vc_str->append("K");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant("E")), (ValueID)2);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant("E")), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant("F")), (ValueID)3);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant("F")), (ValueID)3);

  EXPECT_EQ(dict_col->lower_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
  EXPECT_EQ(dict_col->upper_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
}

TEST_F(StorageFixedStringDictionarySegmentTest, NullValues) {
  std::shared_ptr<ValueSegment<std::string>> vc_str = std::make_shared<ValueSegment<std::string>>(true);

  vc_str->append("A");
  vc_str->append(NULL_VALUE);
  vc_str->append("E");

  auto col = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  auto dict_col = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(col);

  EXPECT_EQ(dict_col->null_value_id(), 2u);
  EXPECT_TRUE(variant_is_null((*dict_col)[1]));
}

TEST_F(StorageFixedStringDictionarySegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */
  const auto empty_compressed_column = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  const auto empty_dictionary_column =
      std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(empty_compressed_column);
  const auto empty_memory_usage = empty_dictionary_column->estimate_memory_usage();

  vc_str->append("A");
  vc_str->append("B");
  vc_str->append("C");
  const auto compressed_column = encode_column(EncodingType::FixedStringDictionary, DataType::String, vc_str);
  const auto dictionary_column = std::dynamic_pointer_cast<FixedStringDictionarySegment<std::string>>(compressed_column);

  static constexpr auto size_of_attribute = 1u;
  static constexpr auto size_of_dictionary = 3u;

  // We have to substract 1 since the empty FixedStringColumn actually contains one null terminator
  EXPECT_EQ(dictionary_column->estimate_memory_usage(),
            empty_memory_usage - 1u + 3 * size_of_attribute + size_of_dictionary);
}

}  // namespace opossum
