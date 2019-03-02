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
  static constexpr auto row_count = 5000u;
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
};

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

  auto offsets = lz4_segment->string_offsets();
  EXPECT_TRUE(offsets.has_value());
  EXPECT_EQ(offsets->size(), 6u);
  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 17, 17};

  for (auto index = 0u; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE(null_values[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offsets)[index] == expected_offsets[index]);
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

  auto offsets = lz4_segment->string_offsets();
  EXPECT_TRUE(offsets.has_value());
  EXPECT_EQ(offsets->size(), 6u);
  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 13, 13};

  for (auto index = 0u; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE(null_values[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offsets)[index] == expected_offsets[index]);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressEmptySegmentString) {
  for (size_t i = 0; i < row_count; ++i) {
    vs_str->append("");
  }

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data.size(), row_count);
  for (const auto& elem : decompressed_data) {
    EXPECT_EQ(elem, "");
  }

  // Test offsets
  auto offsets = lz4_segment->string_offsets();
  EXPECT_TRUE(offsets.has_value());
  EXPECT_EQ(offsets->size(), row_count);
  for (auto offset : (*offsets)) {
    EXPECT_EQ(offset, 0);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressSingleCharSegmentString) {
  for (size_t i = 0; i < row_count; ++i) {
    vs_str->append("");
  }
  vs_str->append("a");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count + 1);

  auto decompressed_data = lz4_segment->decompress();
  auto offsets = lz4_segment->string_offsets();
  EXPECT_TRUE(offsets.has_value());
  EXPECT_EQ(decompressed_data.size(), row_count + 1);
  EXPECT_EQ(offsets->size(), row_count + 1);

  for (auto index = 0u; index < lz4_segment->size() - 1; ++index) {
    // Test compressed values
    EXPECT_EQ(decompressed_data[index], "");

    // Test offsets
    EXPECT_EQ((*offsets)[index], 0);
  }

  // Test last element
  EXPECT_EQ(decompressed_data[5000], "a");
  // This offset is also 0 since the elements before it don't have any content
  EXPECT_EQ((*offsets)[5000], 0);
}

TEST_F(StorageLZ4SegmentTest, CompressZeroOneSegmentString) {
  for (size_t i = 0; i < row_count; ++i) {
    if (i % 2) {
      vs_str->append("0a213123");
    } else {
      vs_str->append("1basdasd");
    }
  }

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<pmr_string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count);

  auto decompressed_data = lz4_segment->decompress();
  auto offsets = lz4_segment->string_offsets();
  EXPECT_TRUE(offsets.has_value());
  EXPECT_EQ(decompressed_data.size(), 6u);
  EXPECT_EQ(offsets->size(), 6u);

  for (auto index = 0u; index < lz4_segment->size() - 1; ++index) {
    // Test offsets
    EXPECT_EQ((*offsets)[index], index);
  }
}

}  // namespace opossum
