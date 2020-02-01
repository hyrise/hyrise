#include "../storage/encoding_test.hpp"

#include "resolve_type.hpp"
#include "storage/base_encoded_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

// In these tests, we make sure that segment encoders properly use the assigned memory resource

// A simple polymorphic memory resource that tracks how much memory was allocated
class SimpleTrackingMemoryResource : public boost::container::pmr::memory_resource {
 public:
  size_t allocated{0};

  void* do_allocate(std::size_t bytes, std::size_t alignment) override {
    allocated += bytes;
    return std::malloc(bytes);  // NOLINT
  }

  void do_deallocate(void* p, std::size_t bytes, std::size_t alignment) override {
    allocated -= bytes;
    std::free(p);  // NOLINT
  }

  bool do_is_equal(const memory_resource& other) const noexcept override { return false; }
};

class SegmentsUsingAllocatorsTest : public ::testing::TestWithParam<std::tuple<DataType, SegmentEncodingSpec>> {
 public:
  void SetUp() override {
    data_type = std::get<0>(GetParam());
    encoding_spec = std::get<1>(GetParam());

    resolve_data_type(data_type, [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;

      const auto convert_value = [](const auto int_value) {
        if constexpr (std::is_same_v<ColumnDataType, pmr_string>) {
          return pmr_string{std::to_string(int_value)};
        } else {
          return int_value;
        }
      };

      original_segment = std::make_shared<ValueSegment<ColumnDataType>>(false, 300);
      empty_original_segment = std::make_shared<ValueSegment<ColumnDataType>>(false, 0);
      // original_segment contains the numbers from 0 to 99, then 100x100, then the numbers from 200 to 299.
      // This way, we can check if, e.g., run-length encoding properly handles the duplicate values
      for (auto i = 0; i <= 99; ++i) original_segment->append(convert_value(i));
      for (auto i = 0; i < 100; ++i) original_segment->append(convert_value(100));
      for (auto i = 200; i <= 299; ++i) original_segment->append(convert_value(i));
    });
  }

  DataType data_type;
  SegmentEncodingSpec encoding_spec;

  std::shared_ptr<BaseValueSegment> original_segment;
  std::shared_ptr<BaseValueSegment> empty_original_segment;
};

TEST_P(SegmentsUsingAllocatorsTest, MigrateSegment) {
  auto encoded_segment = std::static_pointer_cast<BaseSegment>(original_segment);
  if (encoding_spec.encoding_type != EncodingType::Unencoded) {
    encoded_segment = encode_and_compress_segment(original_segment, data_type, encoding_spec);
  }

  auto resource = SimpleTrackingMemoryResource{};
  const auto allocator = PolymorphicAllocator<size_t>(&resource);
  const auto copied_segment = encoded_segment->copy_using_allocator(allocator);

  // The segment control structure (i.e., the object itself) and its members are not stored using PMR. Thus, we
  // retrieve the size of an empty segment for later subtraction.
  auto empty_encoded_segment = std::static_pointer_cast<BaseSegment>(empty_original_segment);
  if (encoding_spec.encoding_type != EncodingType::Unencoded) {
    empty_encoded_segment = encode_and_compress_segment(empty_original_segment, data_type, encoding_spec);
  }

  const auto copied_segment_size = copied_segment->memory_usage(MemoryUsageCalculationMode::Full);
  const auto empty_segment_size = empty_encoded_segment->memory_usage(MemoryUsageCalculationMode::Full);
  EXPECT_GT(copied_segment_size, empty_segment_size);
  const auto estimated_usage = copied_segment_size - empty_segment_size;

  EXPECT_EQ(resource.allocated, estimated_usage);
}

inline std::string segments_using_allocator_test_formatter(
    const testing::TestParamInfo<std::tuple<DataType, SegmentEncodingSpec>>& param_info) {
  std::stringstream stringstream;
  stringstream << std::get<0>(param_info.param);
  stringstream << all_segment_encoding_specs_formatter(
      testing::TestParamInfo<EncodingTest::ParamType>{std::get<1>(param_info.param), 0});
  return stringstream.str();
}

INSTANTIATE_TEST_SUITE_P(Int, SegmentsUsingAllocatorsTest,
                         ::testing::Combine(::testing::Values(DataType::Int),
                                            ::testing::ValuesIn(get_supporting_segment_encodings_specs(DataType::Int,
                                                                                                       true))),
                         segments_using_allocator_test_formatter);

INSTANTIATE_TEST_SUITE_P(String, SegmentsUsingAllocatorsTest,
                         ::testing::Combine(::testing::Values(DataType::String),
                                            ::testing::ValuesIn(get_supporting_segment_encodings_specs(DataType::String,
                                                                                                       true))),
                         segments_using_allocator_test_formatter);

}  // namespace opossum
