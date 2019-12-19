#include "../storage/encoding_test.hpp"

#include "storage/base_encoded_segment.hpp"
#include "storage/segment_encoding_utils.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

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

class SegmentsUsingAllocatorsTest : public EncodingTest {
 public:
  void SetUp() override {
    original_segment = std::make_shared<ValueSegment<int>>();
    empty_original_segment = std::make_shared<ValueSegment<int>>(false, 0);
    // original_segment contains the numbers from 0 to 99, then 100x100, then the numbers from 200 to 299. This way, we
    // can check if, e.g., run-length encoding properly handles the duplicate values
    for (auto i = 0; i < 99; ++i) original_segment->append(i);
    for (auto i = 0; i < 99; ++i) original_segment->append(100);
    for (auto i = 200; i < 299; ++i) original_segment->append(i);
  }

  std::shared_ptr<ValueSegment<int>> original_segment;
  std::shared_ptr<ValueSegment<int>> empty_original_segment;
};

// TODO(md): test strings
// TODO(md): make sure encoding uses same resource

TEST_P(SegmentsUsingAllocatorsTest, MigrateSegment) {
  auto resource = SimpleTrackingMemoryResource{};

  auto encoded_segment = std::static_pointer_cast<BaseSegment>(original_segment);
  auto empty_encoded_segment = std::static_pointer_cast<BaseSegment>(empty_original_segment);
  if (GetParam().encoding_type != EncodingType::Unencoded) {
    encoded_segment = encode_and_compress_segment(original_segment, DataType::Int, GetParam());
    empty_encoded_segment = encode_and_compress_segment(empty_original_segment, DataType::Int, GetParam());
  }

  const auto allocator = PolymorphicAllocator<size_t>(&resource);
  const auto copied_segment = encoded_segment->copy_using_allocator(allocator);

  // The segment control structure (i.e., the object itself) and its members are not stored using PMR. Thus, we
  // subtract the size of an empty segment.
  EXPECT_GT(copied_segment->estimate_memory_usage(), empty_encoded_segment->estimate_memory_usage());
  const auto estimated_usage = copied_segment->estimate_memory_usage() - empty_encoded_segment->estimate_memory_usage();

  EXPECT_EQ(resource.allocated, estimated_usage);
}

INSTANTIATE_TEST_SUITE_P(SegmentsUsingAllocatorsTestInstances, SegmentsUsingAllocatorsTest,
                         ::testing::ValuesIn(std::begin(all_segment_encoding_specs),
                                             std::end(all_segment_encoding_specs)),
                         all_segment_encoding_specs_formatter);

}  // namespace opossum
