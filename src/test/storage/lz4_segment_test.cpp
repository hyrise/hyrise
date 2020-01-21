#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "all_type_variant.hpp"
#include "storage/chunk.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/lz4_segment.hpp"
#include "storage/lz4_segment/lz4_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "types.hpp"

namespace opossum {

class StorageLZ4SegmentTest : public BaseTest {
 protected:
  static constexpr auto row_count = LZ4Encoder::_block_size + size_t{1000u};
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>(true);
  std::shared_ptr<ValueSegment<int>> vs_int = std::make_shared<ValueSegment<int>>(true);
};

template <typename T>
std::shared_ptr<LZ4Segment<T>> compress(std::shared_ptr<ValueSegment<T>> segment, DataType data_type) {
  auto encoded_segment = encode_and_compress_segment(segment, data_type, SegmentEncodingSpec{EncodingType::LZ4});
  return std::dynamic_pointer_cast<LZ4Segment<T>>(encoded_segment);
}

TEST_F(StorageLZ4SegmentTest, HandleOptionalOffsetsAndNullValues) {
  auto empty_int_segment = compress(std::make_shared<ValueSegment<int32_t>>(true), DataType::Int);
  EXPECT_FALSE(empty_int_segment->string_offset_decompressor());
  EXPECT_FALSE(empty_int_segment->null_values());

  auto empty_str_segment = compress(std::make_shared<ValueSegment<pmr_string>>(true), DataType::String);
  EXPECT_FALSE(empty_str_segment->string_offset_decompressor());
  EXPECT_FALSE(empty_str_segment->null_values());

  vs_str->append("Alex");
  vs_str->append("Peter");
  auto str_segment = compress(vs_str, DataType::String);
  EXPECT_TRUE(str_segment->string_offset_decompressor());
  EXPECT_NE(*(str_segment->string_offset_decompressor()), nullptr);
  EXPECT_FALSE(str_segment->null_values());
}

TEST_F(StorageLZ4SegmentTest, CompressEmptyStringNotNullNullableSegment) {
  for (auto index = size_t{0u}; index < row_count; ++index) {
    vs_str->append("");
  }
  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();

  auto& null_values = lz4_segment->null_values();
  EXPECT_FALSE(null_values);

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_FALSE(offset_decompressor);
}

TEST_F(StorageLZ4SegmentTest, CompressNullableStringSegment) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("Hans");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");
  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // Test compressed values
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[0], "Alex");
  EXPECT_EQ(decompressed_data[1], "Peter");

  auto& null_values = lz4_segment->null_values();
  EXPECT_TRUE(null_values);
  EXPECT_EQ(null_values->size(), 6u);
  auto expected_null_values = std::vector<bool>{false, false, false, false, true, false};

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor);
  EXPECT_EQ((*offset_decompressor)->size(), 6u);

  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 17, 17};
  for (auto index = size_t{0u}; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE((*null_values)[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offset_decompressor)->get(index) == expected_offsets[index]);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressNullableAndEmptyStringSegment) {
  vs_str->append("Alex");
  vs_str->append("Peter");
  vs_str->append("Ralf");
  vs_str->append("");
  vs_str->append(NULL_VALUE);
  vs_str->append("Anna");
  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), 6u);

  // The empty string should not be a null value
  auto& null_values = lz4_segment->null_values();
  EXPECT_TRUE(null_values);
  EXPECT_EQ(null_values->size(), 6u);
  auto expected_null_values = std::vector<bool>{false, false, false, false, true, false};

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor);
  EXPECT_EQ((*offset_decompressor)->size(), 6u);

  auto expected_offsets = std::vector<size_t>{0, 4, 9, 13, 13, 13};
  for (auto index = size_t{0u}; index < lz4_segment->size(); ++index) {
    // Test null values
    EXPECT_TRUE((*null_values)[index] == expected_null_values[index]);

    // Test offsets
    EXPECT_TRUE((*offset_decompressor)->get(index) == expected_offsets[index]);
  }
}

TEST_F(StorageLZ4SegmentTest, CompressSingleCharStringSegment) {
  for (auto index = size_t{0u}; index < row_count; ++index) {
    vs_str->append("");
  }
  vs_str->append("a");
  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size
  EXPECT_EQ(lz4_segment->size(), row_count + 1);

  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data.size(), row_count + 1);

  const auto offset_decompressor = lz4_segment->string_offset_decompressor();
  EXPECT_TRUE(offset_decompressor);
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

TEST_F(StorageLZ4SegmentTest, CompressZeroOneStringSegment) {
  for (auto index = size_t{0u}; index < row_count; ++index) {
    vs_str->append(index % 2 ? "0" : "1");
  }
  auto lz4_segment = compress(vs_str, DataType::String);

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

TEST_F(StorageLZ4SegmentTest, CompressMultiBlockStringSegment) {
  const auto block_size = LZ4Encoder::_block_size;
  const auto size_diff = size_t{1000u};
  static_assert(block_size > size_diff, "LZ4 block size is too small");

  // Nearly fills the first block.
  const auto string1 = pmr_string(block_size - size_diff, 'a');
  vs_str->append(string1);
  // Starts in the first block, completely fills second block and reaches the third block.
  const auto string2 = pmr_string(block_size + (2 * size_diff), 'b');
  vs_str->append(string2);
  // Stays in the third block.
  const auto string3 = pmr_string(size_diff, 'c');
  vs_str->append(string3);
  const auto third_block_size = (string1.size() + string2.size() + string3.size()) % block_size;

  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size.
  EXPECT_EQ(lz4_segment->size(), 3u);

  // Test element wise decompression without caching.
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{1u}), string2);
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{2u}), string3);
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{0u}), string1);

  // Test element wise decompression with cache.
  auto cache = std::vector<char>{};
  std::pair<pmr_string, size_t> result;

  // First access the third block (cache miss).
  result = lz4_segment->decompress(ChunkOffset{2u}, std::nullopt, cache);
  EXPECT_EQ(cache.size(), third_block_size);
  EXPECT_EQ(result.first, string3);
  EXPECT_EQ(result.second, 2u);

  /**
   * Access the first, second and third block. The cache should be used for the third block. As a result the buffer
   * used for decompression will contain the second block since it needed to be decompressed.
   */
  result = lz4_segment->decompress(ChunkOffset{1u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, string2);
  EXPECT_EQ(result.second, 1u);

  // Access the same blocks again. Now, the cache should be used for the second block and then contain the third block.
  result = lz4_segment->decompress(ChunkOffset{1u}, result.second, cache);
  EXPECT_EQ(cache.size(), third_block_size);
  EXPECT_EQ(result.first, string2);
  EXPECT_EQ(result.second, 2u);

  // Access the first block (cache miss).
  result = lz4_segment->decompress(ChunkOffset{0u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, string1);
  EXPECT_EQ(result.second, 0u);

  /**
   * Access the first, second and third block again. The cache should now be used for the first block and afterwards be
   * overwritten with the third block.
   */
  result = lz4_segment->decompress(ChunkOffset{1u}, result.second, cache);
  EXPECT_EQ(cache.size(), third_block_size);
  EXPECT_EQ(result.first, string2);
  EXPECT_EQ(result.second, 2u);
}

TEST_F(StorageLZ4SegmentTest, CompressDictionaryStringSegment) {
  const auto block_size = LZ4Encoder::_block_size;
  const auto num_rows = Chunk::DEFAULT_SIZE / 20;

  for (auto index = size_t{0u}; index < num_rows; ++index) {
    vs_str->append(AllTypeVariant{pmr_string{"this is element " + std::to_string(index)}});
  }

  auto lz4_segment = compress(vs_str, DataType::String);

  // Test segment size.
  EXPECT_EQ(lz4_segment->size(), num_rows);

  // A dictionary should exist.
  EXPECT_FALSE(lz4_segment->dictionary().empty());

  // Access elements without cache
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{2u}), "this is element 2");
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{4013u}), "this is element 4013");
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{200u}), "this is element 200");

  // Access elements with cache
  auto cache = std::vector<char>{};
  std::pair<pmr_string, size_t> result;

  result = lz4_segment->decompress(ChunkOffset{4102u}, std::nullopt, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, "this is element 4102");

  result = lz4_segment->decompress(ChunkOffset{4104u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, "this is element 4104");

  result = lz4_segment->decompress(ChunkOffset{3003u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, "this is element 3003");

  result = lz4_segment->decompress(ChunkOffset{num_rows - 1}, result.second, cache);
  EXPECT_EQ(result.first, pmr_string{"this is element " + std::to_string(num_rows - 1)});

  // Finally, decompress the whole segment.
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[1234], "this is element 1234");
  EXPECT_EQ(decompressed_data[4312], "this is element 4312");
  EXPECT_EQ(decompressed_data[4014], "this is element 4014");
}

TEST_F(StorageLZ4SegmentTest, CompressDictionaryIntSegment) {
  const auto block_size = LZ4Encoder::_block_size;
  const auto num_rows = Chunk::DEFAULT_SIZE / 4;

  for (auto index = size_t{0u}; index < num_rows; ++index) {
    vs_int->append(static_cast<int>(index * 2));
  }

  auto lz4_segment = compress(vs_int, DataType::Int);

  // Test segment size.
  EXPECT_EQ(lz4_segment->size(), num_rows);

  // A dictionary should exist.
  EXPECT_FALSE(lz4_segment->dictionary().empty());

  // Access elements without cache
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{1u}), 2);
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{10123u}), 20246);
  EXPECT_EQ(lz4_segment->decompress(ChunkOffset{200u}), 400);

  // Access elements with cache
  auto cache = std::vector<char>{};
  std::pair<int, size_t> result;

  result = lz4_segment->decompress(ChunkOffset{20123u}, std::nullopt, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, 40246);

  result = lz4_segment->decompress(ChunkOffset{20124u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, 40248);

  result = lz4_segment->decompress(ChunkOffset{3003u}, result.second, cache);
  EXPECT_EQ(cache.size(), block_size);
  EXPECT_EQ(result.first, 6006);

  result = lz4_segment->decompress(ChunkOffset{num_rows - 1}, result.second, cache);
  EXPECT_EQ(result.first, 2 * (num_rows - 1));

  // Finally, decompress the whole segment.
  auto decompressed_data = lz4_segment->decompress();
  EXPECT_EQ(decompressed_data[1234], 2468);
  EXPECT_EQ(decompressed_data[4312], 8624);
  EXPECT_EQ(decompressed_data[20124], 40248);
}

}  // namespace opossum
