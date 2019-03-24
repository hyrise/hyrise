#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/chunk_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

class StorageLZ4SegmentTest : public BaseTest {
 protected:
  static constexpr auto row_count = size_t{17000u};
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
};

template <typename T>
std::shared_ptr<LZ4Segment<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = encode_segment(EncodingType::LZ4, data_type, segment);
  return std::dynamic_pointer_cast<LZ4Segment<T>>(encoded_segment);
}

TEST_F(StorageLZ4SegmentTest, HandleOffsetsInEmptySegment) {
  auto empty_int_segment = compress(std::make_shared<ValueSegment<int32_t>>(true), DataType::Int);
  EXPECT_EQ(empty_int_segment->string_offset_decompressor(), std::nullopt);

  auto empty_str_segment = compress(std::make_shared<ValueSegment<pmr_string>>(true), DataType::String);
  EXPECT_EQ(empty_str_segment->string_offset_decompressor(), std::nullopt);

  vs_str->append("Alex");
  vs_str->append("Peter");
  auto str_segment = compress(vs_str, DataType::String);
  EXPECT_TRUE(str_segment->string_offset_decompressor().has_value());
  EXPECT_NE(*str_segment->string_offset_decompressor(), nullptr);
}

TEST_F(StorageLZ4SegmentTest, CompressNullableSegmentString) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("Hans");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[0], "Alex");
  EXPECT_EQ(decompressed_data[1], "Peter");

  auto& null_values = lz4_segment->null_values();
  EXPECT_EQ(null_values.size(), 6u);
  auto expected_null_values = std::vector<bool>{false, false, false, false, true, false};

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor.has_value());
  EXPECT_EQ((*offset_decompressor)->size(), 6u);

  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 17, 17};
  for (auto index = size_t{0u}; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE(null_values[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offset_decompressor)->get(index) == expected_offsets[index]);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressNullableAndEmptySegmentString) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // The empty string should not be a null value
  auto& null_values = lz4_segment->null_values();
  EXPECT_EQ(null_values.size(), 6u);
  auto expected_null_values = std::vector<bool>{false, false, false, false, true, false};

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor.has_value());
  EXPECT_EQ((*offset_decompressor)->size(), 6u);

  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 13, 13};
  for (auto index = size_t{0u}; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE(null_values[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offset_decompressor)->get(index) == expected_offsets[index]);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressSingleCharSegmentString) {
  for (auto index = size_t{0u}; index < row_count; ++index) {
    vs_str->append("");
  }
  vs_str->append("a");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count + 1);

  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data.size(), row_count + 1);

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor.has_value());
  EXPECT_EQ((*offset_decompressor)->size(), row_count + 1);

  for (auto index = size_t{0u}; index < lz4_segment->size() - 1; ++index) {
    // Test compressed values
    EXPECT_EQ(decompressed_data[index], "");

    // Test offsets
    EXPECT_EQ((*offset_decompressor)->get(index), 0);
  }

  // Test last element
  EXPECT_EQ(decompressed_data[row_count], "a");
  // This offset is also 0 since the elements before it don't have any content
  EXPECT_EQ((*offset_decompressor)->get(row_count), 0);
}

TEST_F(StorageLZ4SegmentTest, CompressZeroOneSegmentString) {
  for (auto index = size_t{0u}; index < row_count; ++index) {
    vs_str->append(index % 2 ? "0" : "1");
  }

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count);
  EXPECT_TRUE(lz4_segment->dictionary().empty());

  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data.size(), row_count);

  // Test element values
  for (auto index = size_t{0u}; index < lz4_segment->size(); ++index) {
    EXPECT_EQ(decompressed_data[index], index % 2 ? "0" : "1");
  }
}

}  // namespace opossum
