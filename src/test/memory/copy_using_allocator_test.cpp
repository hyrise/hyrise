#include <boost/algorithm/string/classification.hpp>
#include <boost/range/algorithm_ext/erase.hpp>

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

class CopyUsingAllocatorTest : public EncodingTest {};

// TODO add test that encoding remains on same node

TEST_P(CopyUsingAllocatorTest, MigrateSegment) {
  auto resource = SimpleTrackingMemoryResource{};

  const auto original_segment = std::make_shared<ValueSegment<int>>();
  original_segment->append(7);
  original_segment->append(8);
  original_segment->append(9);

  auto encoded_segment = std::static_pointer_cast<BaseSegment>(original_segment);
  if (GetParam().encoding_type != EncodingType::Unencoded) {
    encoded_segment = encode_and_compress_segment(original_segment, DataType::Int, GetParam());
  }

  const auto allocator = PolymorphicAllocator<size_t>(&resource);
  const auto copied_segment = encoded_segment->copy_using_allocator(allocator);

  std::cout << resource.allocated << " / " << copied_segment->estimate_memory_usage() << std::endl;
}

INSTANTIATE_TEST_CASE_P(CopyUsingAllocatorTestInstances, CopyUsingAllocatorTest,
                        ::testing::ValuesIn(std::begin(all_segment_encoding_specs),
                                            std::end(all_segment_encoding_specs)),
                        [](const testing::TestParamInfo<EncodingTest::ParamType>& param_info) {  // TODO make global
                          std::stringstream stringstream;
                          stringstream << param_info.param;
                          auto string = stringstream.str();
                          boost::remove_erase_if(string, boost::is_any_of("() -"));
                          return string;
                        });

}  // namespace opossum
