#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/segment_encoding_utils.hpp"

namespace opossum {

class AnySegmentIterableTest : public BaseTestWithParam<SegmentEncodingSpec> {
 public:
  static void SetUpTestCase() {
    int_values = {1, 2, 2, 2, 5, 2, 2, 8, 2, 2};
    float_values = {1.5f, 2.5f, 3.5f, 4.5f, 5.5f, 6.5f, 7.5f, 8.5f, 9.5f, 10.5f};
    string_values = {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j"};
    null_values = {false, true, true, false, false, true, false, true, true, false};

    auto row_ids = PosList{
      {ChunkID{0}, ChunkOffset{0}},
      {ChunkID{0}, ChunkOffset{8}},
      {ChunkID{0}, ChunkOffset{7}},
      {ChunkID{0}, ChunkOffset{1}},
      {ChunkID{0}, ChunkOffset{1}},
      {ChunkID{0}, ChunkOffset{5}}
    };
    position_filter = std::make_shared<PosList>(std::move(row_ids));
    position_filter->guarantee_single_chunk();
  }

  void SetUp() override {
    const auto param = GetParam();

    int_segment = std::make_shared<ValueSegment<int32_t>>(int_values);

    if (param.encoding_type != EncodingType::Unencoded) {
      if (param.vector_compression_type) {
        int_segment = encode_segment(param.encoding_type,
                                     DataType::Int,
                                     std::dynamic_pointer_cast<ValueSegment<int32_t>>(int_segment),
                                     *param.vector_compression_type);
      } else {
        int_segment = encode_segment(param.encoding_type,
                                     DataType::Int,
                                     std::dynamic_pointer_cast<ValueSegment<int32_t>>(int_segment));
      }
    }
  }

 protected:
  std::shared_ptr<BaseSegment> int_segment;
  std::shared_ptr<BaseSegment> float_segment;
  std::shared_ptr<BaseSegment> string_segment;

  inline static std::vector<int32_t> int_values;
  inline static std::vector<float> float_values;
  inline static std::vector<std::string> string_values;
  inline static std::vector<bool> null_values;
  inline static std::shared_ptr<PosList> position_filter;
};

TEST_P(AnySegmentIterableTest, Int) {
  auto any_segment_iterable_int = create_any_segment_iterable<int32_t>(*int_segment);

  auto values = std::vector<int32_t>{};
  any_segment_iterable_int.for_each([&](const auto& value) {
    values.emplace_back(value.value());
  });


  EXPECT_EQ(values, int_values);
}

TEST_P(AnySegmentIterableTest, IntWithPositionFilter) {
  auto any_segment_iterable_int = create_any_segment_iterable<int32_t>(*int_segment);

  auto index = size_t{0};
  any_segment_iterable_int.for_each(position_filter, [&](const auto& value) {
    EXPECT_EQ(value.value(), int_values[(*position_filter)[index].chunk_offset]);
    ++index;
  });

  EXPECT_EQ(index, position_filter->size());
}

INSTANTIATE_TEST_CASE_P(AnySegmentIterableTestInstances,
                        AnySegmentIterableTest,
                        ::testing::Values(
                          SegmentEncodingSpec{EncodingType::Unencoded},
                          SegmentEncodingSpec{EncodingType::Dictionary},
                          SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::FixedSizeByteAligned},
                          SegmentEncodingSpec{EncodingType::Dictionary, VectorCompressionType::SimdBp128}
                        ), );

}  // namespace opossum
