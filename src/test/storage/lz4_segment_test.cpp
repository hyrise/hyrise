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
  std::shared_ptr<ValueSegment<std::string>> vs_str = std::make_shared<ValueSegment<std::string>>();
};

TEST_F(EncodedSegmentTest, CompressNullableSegmentString) {
  vs_str = std::make_shared<ValueSegment<std::string>>(true);

  vs_str->append("a");
  vs_str->append("b");
  vs_str->append("a");
  vs_str->append("b");
  vs_str->append(NULL_VALUE);
  vs_str->append("b");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<std::string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto compressed_values = lz4_segment->compressed_data();
  EXPECT_EQ((*compressed_values)[0], "a");
  EXPECT_EQ((*compressed_values)[1], "b");

  // Test null value
  EXPECT_TRUE(variant_is_null((*lz4_segment)[4]));
}

TEST_F(EncodedSegmentTest, CompressNullableAndEmptySegmentString) {
  vs_str = std::make_shared<ValueSegment<std::string>>(true);

  vs_str->append("a");
  vs_str->append("b");
  vs_str->append("a");
  vs_str->append("b");
  vs_str->append("");
  vs_str->append(NULL_VALUE);
  vs_str->append("b");

  auto segment = encode_segment(EncodingType::LZ4, DataType::String, vs_str);
  auto lz4_segment = std::dynamic_pointer_cast<LZ4Segment<std::string>>(segment);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto compressed_values = lz4_segment->compressed_data();
  EXPECT_EQ((*compressed_values)[0], "a");
  EXPECT_EQ((*compressed_values)[1], "b");

  // Test null value
  EXPECT_TRUE(variant_is_null((*lz4_segment)[5]));

  // Test offsets
  auto offsets = lz4_segment->offsets();
  const pmr_vector<size_t> expected_offsets = {0, 1, 2, 3, 4, 4, 4};

  for (std::size_t pos = 0; pos < offsets->size(); pos++) {
    EXPECT_EQ((*offsets).at(pos), expected_offsets.at(pos));
  }
}

TEST_F(EncodedSegmentTest, CompressEmptySegmentString) {
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

  // Test offsets
  auto offsets = lz4_segment->offsets();
  const pmr_vector<size_t> expected_offsets = {0, 0, 0, 0, 0, 0};

  for (std::size_t pos = 0; pos < offsets->size(); pos++) {
    EXPECT_EQ((*offsets).at(pos), expected_offsets.at(pos));
  }
}

}  // namespace opossum
