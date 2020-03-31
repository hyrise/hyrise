#include <memory>
#include <random>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "storage/encoding_test.hpp"

#include "storage/chunk_encoder.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

class AnySegmentIterableTest : public BaseTestWithParam<SegmentEncodingSpec> {
 public:
  static void SetUpTestCase() {
    int_values = {1, 2, 2, 2, 5, 2, 2, 8, 2, 2};

    auto row_ids =
        RowIDPosList{{ChunkID{0}, ChunkOffset{0}}, {ChunkID{0}, ChunkOffset{8}}, {ChunkID{0}, ChunkOffset{7}},
                     {ChunkID{0}, ChunkOffset{1}}, {ChunkID{0}, ChunkOffset{1}}, {ChunkID{0}, ChunkOffset{5}}};
    position_filter = std::make_shared<RowIDPosList>(std::move(row_ids));
    position_filter->guarantee_single_chunk();
  }

  void SetUp() override {
    const auto param = GetParam();

    auto segment_encoding_spec = SegmentEncodingSpec{param.encoding_type};
    if (param.vector_compression_type) {
      segment_encoding_spec.vector_compression_type = *param.vector_compression_type;
    }
    auto int_values_copy = int_values;
    const auto value_int_segment = std::make_shared<ValueSegment<int32_t>>(std::move(int_values_copy));

    int_segment = ChunkEncoder::encode_segment(std::dynamic_pointer_cast<ValueSegment<int32_t>>(value_int_segment),
                                               DataType::Int, segment_encoding_spec);
  }

 protected:
  std::shared_ptr<BaseSegment> int_segment;

  inline static pmr_vector<int32_t> int_values;
  inline static std::shared_ptr<RowIDPosList> position_filter;
};

TEST_P(AnySegmentIterableTest, Int) {
  auto any_segment_iterable_int = create_any_segment_iterable<int32_t>(*int_segment);

  auto values = pmr_vector<int32_t>{};
  any_segment_iterable_int.for_each([&](const auto& position) { values.emplace_back(position.value()); });

  EXPECT_EQ(values, int_values);
}

TEST_P(AnySegmentIterableTest, IntWithPositionFilter) {
  auto any_segment_iterable_int = create_any_segment_iterable<int32_t>(*int_segment);

  auto index = size_t{0};
  any_segment_iterable_int.for_each(position_filter, [&](const auto& position) {
    EXPECT_EQ(position.value(), int_values[(*position_filter)[index].chunk_offset]);
    ++index;
  });

  EXPECT_EQ(index, position_filter->size());
}

auto any_segment_iterable_test_formatter = [](const ::testing::TestParamInfo<SegmentEncodingSpec> info) {
  const auto spec = info.param;

  auto stream = std::stringstream{};
  stream << spec.encoding_type;
  if (spec.vector_compression_type) {
    stream << "-" << *spec.vector_compression_type;
  }

  auto string = stream.str();
  string.erase(std::remove_if(string.begin(), string.end(), [](char c) { return !std::isalnum(c); }), string.end());

  return string;
};

INSTANTIATE_TEST_SUITE_P(AnySegmentIterableTestInstances, AnySegmentIterableTest,
                         ::testing::ValuesIn(get_supporting_segment_encodings_specs(DataType::Int, true)),
                         any_segment_iterable_test_formatter);
}  // namespace opossum
