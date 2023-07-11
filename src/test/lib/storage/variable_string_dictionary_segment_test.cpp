#include <memory>
#include <string>
#include <utility>

#include "base_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"
#include "storage/variable_string_dictionary_segment.hpp"
#include "storage/vector_compression/fixed_width_integer/fixed_width_integer_vector.hpp"

namespace hyrise {

class StorageVariableStringDictionarySegmentTest : public BaseTest {
 protected:
  std::shared_ptr<ValueSegment<pmr_string>> vs_str = std::make_shared<ValueSegment<pmr_string>>();
};

TEST_F(StorageVariableStringDictionarySegmentTest, CompressSegmentString) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Alexander");
  vs_str->append("Steve");
  vs_str->append("Hasso");
  vs_str->append("Bill");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  // Test attribute_vector size
  EXPECT_EQ(dict_segment->size(), 6u);
  EXPECT_EQ(dict_segment->attribute_vector()->size(), 6u);

  // Test dictionary size (uniqueness)
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);

  // Test sorting
  auto dict = dict_segment->dictionary();
  // TODO: Replace with working code
  //  EXPECT_EQ(*(dict->begin()), "Alexander");
  //  EXPECT_EQ(*(dict->begin() + 1), "Bill");
  //  EXPECT_EQ(*(dict->begin() + 2), "Hasso");
  //  EXPECT_EQ(*(dict->begin() + 3), "Steve");
}

TEST_F(StorageVariableStringDictionarySegmentTest, Decode) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Bill");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  EXPECT_EQ(dict_segment->encoding_type(), EncodingType::VariableStringDictionary);
  EXPECT_EQ(dict_segment->compressed_vector_type(), CompressedVectorType::FixedWidthInteger1Byte);

  // Decode values
  EXPECT_EQ((*dict_segment)[ChunkOffset{0}], AllTypeVariant("Bill"));
  EXPECT_EQ((*dict_segment)[ChunkOffset{1}], AllTypeVariant("Steve"));
  EXPECT_EQ((*dict_segment)[ChunkOffset{2}], AllTypeVariant("Bill"));
}

TEST_F(StorageVariableStringDictionarySegmentTest, LongStrings) {
  vs_str->append("ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
  vs_str->append("QuiteShort");
  vs_str->append("Short");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  // Test sorting
  auto dict = dict_segment->dictionary();
  // TODO: Here
  //  EXPECT_EQ(*(dict->begin()), "QuiteShort");
  //  EXPECT_EQ(*(dict->begin() + 1), "Short");
  //  EXPECT_EQ(*(dict->begin() + 2), "ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
}

TEST_F(StorageVariableStringDictionarySegmentTest, LowerUpperBound) {
  vs_str->append("A");
  vs_str->append("C");
  vs_str->append("E");
  vs_str->append("G");
  vs_str->append("I");
  vs_str->append("K");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  // Test for AllTypeVariant as parameter
  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("E")), ValueID{2});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("E")), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("F")), ValueID{3});
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("F")), ValueID{3});

  EXPECT_EQ(dict_segment->lower_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
  EXPECT_EQ(dict_segment->upper_bound(AllTypeVariant("Z")), INVALID_VALUE_ID);
}

TEST_F(StorageVariableStringDictionarySegmentTest, NullValues) {
  const auto vs_str = std::make_shared<ValueSegment<pmr_string>>(true);

  vs_str->append("A");
  vs_str->append(NULL_VALUE);
  vs_str->append("E");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  EXPECT_EQ(dict_segment->null_value_id(), 2u);
  EXPECT_TRUE(variant_is_null((*dict_segment)[ChunkOffset{1}]));
}

TEST_F(StorageVariableStringDictionarySegmentTest, MemoryUsageEstimation) {
  /**
   * WARNING: Since it's hard to assert what constitutes a correct "estimation", this just tests basic sanity of the
   * memory usage estimations
   */
  const auto empty_compressed_segment = ChunkEncoder::encode_segment(
      vs_str, DataType::String, SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto empty_dictionary_segment =
      std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(empty_compressed_segment);
  const auto empty_memory_usage = empty_dictionary_segment->memory_usage(MemoryUsageCalculationMode::Full);

  vs_str->append("A");
  vs_str->append("B");
  vs_str->append("C");
  const auto compressed_segment = ChunkEncoder::encode_segment(
      vs_str, DataType::String, SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dictionary_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(compressed_segment);

  static constexpr auto size_of_attribute = 1u;
  static constexpr auto size_of_dictionary = 3u;

  // We have to substract 1 since the empty VariableStringSegment actually contains one null terminator
  EXPECT_EQ(dictionary_segment->memory_usage(MemoryUsageCalculationMode::Full),
            empty_memory_usage - 1u + 3 * size_of_attribute + size_of_dictionary);
}

TEST_F(StorageVariableStringDictionarySegmentTest, TestOffsetVector) {
  vs_str->append("ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
  vs_str->append("QuiteShort");
  vs_str->append("Short");

  auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);
  const auto offset_vector = dict_segment->offset_vector();
  EXPECT_EQ(offset_vector->size(), 3);
  // TODO: Add more tests.
}

TEST_F(StorageVariableStringDictionarySegmentTest, TestLookup) {
  const auto allocator = PolymorphicAllocator<pmr_string>{};
  // 1. Create string data (klotz)
  // Contains zero-length string at the end, just to be annoying.
  const auto data = std::array<char, 30>{"Hello\0World\0Alexander\0String\0"};
  const auto klotz = std::make_shared<pmr_vector<char>>();
  klotz->resize(data.size());
  std::memcpy(klotz->data(), data.data(), data.size());
  const pmr_vector<uint32_t> offsets{0, 6, 12, 22, 29};
  const pmr_vector<uint32_t> attribute_vector{0, 0, 6, 22, 12, 29, 12};
  const auto segment =
      VariableStringDictionarySegment<pmr_string>{klotz,
                                      std::shared_ptr<const BaseCompressedVector>(compress_vector(
                                          attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {4})),
                                      std::make_shared<pmr_vector<uint32_t>>(offsets)};

  auto accessors = std::vector<std::function<AllTypeVariant(const VariableStringDictionarySegment<pmr_string>&, const ChunkOffset)>>{
                                           +[](const VariableStringDictionarySegment<pmr_string>& segment, const ChunkOffset offset) {
                                             const auto maybe = segment.get_typed_value(offset);
                                             return maybe ? maybe.value() : NULL_VALUE;
                                           },
                                           +[](const VariableStringDictionarySegment<pmr_string>& segment, const ChunkOffset offset) {
                                             return segment[offset];
                                           }};
  for (const auto& accessor : accessors) {
    EXPECT_EQ(accessor(segment, ChunkOffset{0}), AllTypeVariant{"Hello"});
    EXPECT_EQ(accessor(segment, ChunkOffset{1}), AllTypeVariant{"Hello"});
    EXPECT_EQ(accessor(segment, ChunkOffset{2}), AllTypeVariant{"World"});
    EXPECT_EQ(accessor(segment, ChunkOffset{3}), AllTypeVariant{"String"});
    EXPECT_EQ(accessor(segment, ChunkOffset{4}), AllTypeVariant{"Alexander"});
    EXPECT_EQ(accessor(segment, ChunkOffset{5}), AllTypeVariant{""});
    EXPECT_EQ(accessor(segment, ChunkOffset{6}), AllTypeVariant{"Alexander"});
  }
}

TEST_F(StorageVariableStringDictionarySegmentTest, TestIterable) {
  auto value_segment = std::make_shared<ValueSegment<pmr_string>>(true);
  value_segment->append("Bill");
  value_segment->append("");
  value_segment->append("Steve");
  value_segment->append(NULL_VALUE);
  value_segment->append("Bill");
  auto segment = ChunkEncoder::encode_segment(value_segment, DataType::String,
                                              SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  auto iterable = create_iterable_from_segment<pmr_string>(*dict_segment);
  auto current_chunk_offset = ChunkOffset{0};
  iterable.for_each([&](const auto& value) {
      const auto expected_value = value_segment->operator[](current_chunk_offset);
      current_chunk_offset++;
      if (variant_is_null(expected_value)) {
        EXPECT_TRUE(value.is_null());
        return;
      }
      ASSERT_FALSE(value.is_null());
      EXPECT_EQ(value.value(), boost::get<pmr_string>(expected_value));
  });
}

}  // namespace hyrise
