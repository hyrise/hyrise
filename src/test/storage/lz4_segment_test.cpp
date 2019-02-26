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
  std::shared_ptr<ValueSegment<std::string>> vs_str = std::make_shared<ValueSegment<std::string>>(true);
};

TEST_F(StorageLZ4SegmentTest, CompressNullableSegmentString) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("Hans");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<std::string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[0], "Alex");
  EXPECT_EQ(decompressed_data[1], "Peter");

  // Test null value
  EXPECT_TRUE(variant_is_null(decompressed_data[4]));

  // Test offsets
  auto offsets = *lz4_segment->offsets();
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 4);
  EXPECT_EQ(offsets[2], 9);
  EXPECT_EQ(offsets[3], 13);
  EXPECT_EQ(offsets[4], 17);
  EXPECT_EQ(offsets[5], 17);
}

TEST_F(StorageLZ4SegmentTest, CompressNullableAndEmptySegmentString) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<std::string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[0], "Alex");
  EXPECT_EQ(decompressed_data[1], "Peter");

  // Test null value
  EXPECT_FALSE(variant_is_null(decompressed_data[3]));
  EXPECT_TRUE(variant_is_null(decompressed_data[4]));

  // Test offsets
  auto offsets = *lz4_segment->offsets();
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 4);
  EXPECT_EQ(offsets[2], 9);
  EXPECT_EQ(offsets[3], 13);
  EXPECT_EQ(offsets[4], 13);
  EXPECT_EQ(offsets[5], 13);
}

TEST_F(StorageLZ4SegmentTest, CompressEmptySegmentString) {
  vs_str->append("");
  vs_str->append("");
  vs_str->append("");
  vs_str->append("");
  vs_str->append("");
  vs_str->append("");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<std::string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[0], "");
  EXPECT_EQ(decompressed_data[3], "");

  // Test offsets
  auto offsets = *lz4_segment->offsets();
  for (auto offset : offsets) {
    EXPECT_EQ(offset, 0);
  }
}

}  // namespace opossum
