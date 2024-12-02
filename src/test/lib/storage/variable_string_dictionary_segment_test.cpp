#include <memory>
#include <string>

#include "base_test.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/value_segment.hpp"
#include "storage/variable_string_dictionary/variable_string_vector.hpp"
#include "storage/variable_string_dictionary/variable_string_vector_iterator.hpp"
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
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  // Test attribute_vector size.
  EXPECT_EQ(dict_segment->size(), 6u);
  EXPECT_EQ(dict_segment->attribute_vector()->size(), 6u);

  // Test dictionary size (uniqueness).
  EXPECT_EQ(dict_segment->unique_values_count(), 4u);
}

TEST_F(StorageVariableStringDictionarySegmentTest, Decode) {
  vs_str->append("Bill");
  vs_str->append("Steve");
  vs_str->append("Bill");

  const auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  EXPECT_EQ(dict_segment->encoding_type(), EncodingType::VariableStringDictionary);
  EXPECT_EQ(dict_segment->compressed_vector_type(), CompressedVectorType::FixedWidthInteger1Byte);

  // Decode values.
  EXPECT_EQ((*dict_segment)[ChunkOffset{0}], AllTypeVariant("Bill"));
  EXPECT_EQ((*dict_segment)[ChunkOffset{1}], AllTypeVariant("Steve"));
  EXPECT_EQ((*dict_segment)[ChunkOffset{2}], AllTypeVariant("Bill"));
}

TEST_F(StorageVariableStringDictionarySegmentTest, LowerUpperBound) {
  vs_str->append("A");
  vs_str->append("C");
  vs_str->append("E");
  vs_str->append("G");
  vs_str->append("I");
  vs_str->append("K");

  const auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

  // Test for AllTypeVariant as parameter.
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

  const auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);

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
  const auto dictionary_segment =
      std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(compressed_segment);

  static constexpr auto size_of_attribute_vector_entry = 1u;
  // 3u for letters and 3u for null terminators
  static constexpr auto size_of_dictionary = 6u;
  static constexpr auto size_of_offset_vector = sizeof(uint32_t);

  EXPECT_EQ(dictionary_segment->memory_usage(MemoryUsageCalculationMode::Full),
            empty_memory_usage + 3 * (size_of_attribute_vector_entry + size_of_offset_vector) + size_of_dictionary);
}

TEST_F(StorageVariableStringDictionarySegmentTest, TestOffsetVector) {
  vs_str->append("ThisIsAVeryLongStringThisIsAVeryLongStringThisIsAVeryLongString");
  vs_str->append("QuiteShort");
  vs_str->append("Short");

  const auto segment = ChunkEncoder::encode_segment(vs_str, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);
  const auto offset_vector = dict_segment->offset_vector();
  EXPECT_EQ(offset_vector.size(), 3);
}

TEST_F(StorageVariableStringDictionarySegmentTest, TestLookup) {
  const auto allocator = PolymorphicAllocator<pmr_string>{};
  // Create string data for clob.
  // Contains zero-length string at the end, just to be annoying.
  const auto data = std::array<char, 30>{"Hello\0World\0Alexander\0String\0"};
  const auto clob_size = data.size();
  auto clob = pmr_vector<char>(clob_size);
  std::memcpy(clob.data(), data.data(), clob_size);
  auto offsets = pmr_vector<uint32_t>{0, 6, 12, 22, 29};
  const auto attribute_vector = pmr_vector<uint32_t>{0, 0, 1, 3, 2, 4, 2};

  const auto segment = VariableStringDictionarySegment<pmr_string>{
      std::move(clob), compress_vector(attribute_vector, VectorCompressionType::FixedWidthInteger, allocator, {4}),
      std::move(offsets)};

  const auto accessors =
      std::vector<std::function<AllTypeVariant(const VariableStringDictionarySegment<pmr_string>&, const ChunkOffset)>>{
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
  const auto value_segment = std::make_shared<ValueSegment<pmr_string>>(true);
  value_segment->append("Bill");
  value_segment->append("");
  value_segment->append("Steve");
  value_segment->append(NULL_VALUE);
  value_segment->append("Bill");

  const auto segment = ChunkEncoder::encode_segment(value_segment, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);
  const auto iterable = create_iterable_from_segment<pmr_string>(*dict_segment);
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

TEST_F(StorageVariableStringDictionarySegmentTest, TestVectorIterator) {
  const auto value_segment = std::make_shared<ValueSegment<pmr_string>>(true);
  value_segment->append("Bill");
  value_segment->append("");
  value_segment->append("Steve");
  value_segment->append(NULL_VALUE);
  value_segment->append("Bill");

  const auto segment = ChunkEncoder::encode_segment(value_segment, DataType::String,
                                                    SegmentEncodingSpec{EncodingType::VariableStringDictionary});
  const auto dict_segment = std::dynamic_pointer_cast<VariableStringDictionarySegment<pmr_string>>(segment);
  const auto variable_string_vector = dict_segment->variable_string_dictionary();
  auto it = variable_string_vector.begin();

  EXPECT_EQ("", *it++);
  EXPECT_EQ("Bill", *it++);
  EXPECT_EQ("Steve", *it++);

  const auto first = variable_string_vector.begin();
  const auto second = first + 1;
  auto third = first + 2;

  EXPECT_EQ("", *first);
  EXPECT_EQ("Bill", *second);
  EXPECT_EQ("Steve", *third);

  EXPECT_EQ(1, second - first);
  EXPECT_EQ(2, third - first);
  EXPECT_EQ(-1, first - second);

  EXPECT_EQ("Bill", *--third);
}

}  // namespace hyrise
