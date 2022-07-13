#include "base_test.hpp"
#include "memory/tracking_memory_resource.hpp"

namespace opossum {

class TrackingMemoryResourceTest : public BaseTest {
 protected:
  TrackingMemoryResource tracking_memory_resource;
};

TEST_F(TrackingMemoryResourceTest, PerformAllocations) {
  auto int_ptr = reinterpret_cast<int*>(tracking_memory_resource.allocate(sizeof(int)));
  EXPECT_NO_THROW((*int_ptr) = 1);

  auto float_ptr = reinterpret_cast<float*>(tracking_memory_resource.allocate(sizeof(float) * 2));
  EXPECT_NO_THROW(float_ptr[0] = 1);
  EXPECT_NO_THROW(float_ptr[1] = 2);
}

TEST_F(TrackingMemoryResourceTest, RecordAllocations) {
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 0);

  tracking_memory_resource.allocate(sizeof(int));
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_int, recorded_amount_int] = tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_int, sizeof(int));
  EXPECT_LT(recorded_time_int, std::chrono::system_clock::now());
  EXPECT_GT(recorded_time_int, std::chrono::system_clock::now() - std::chrono::milliseconds(100));

  tracking_memory_resource.allocate(sizeof(float) * 2);
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 2);
  const auto& [recorded_time_float, recorded_amount_float] = tracking_memory_resource.memory_timeseries()[1];
  EXPECT_EQ(recorded_amount_float, sizeof(float) * 2);
  EXPECT_LT(recorded_time_float, std::chrono::system_clock::now());
  EXPECT_GT(recorded_time_float, std::chrono::system_clock::now() - std::chrono::milliseconds(100));
}

/**
 * Test that free was called by using mock
 *   (expect that was called exactly once with right pointer)
 * necessary test (not just nice-to-have)
 */

TEST_F(TrackingMemoryResourceTest, RecordDeallocations) {
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 0);

  auto mem_ptr_1 = std::malloc(10);
  tracking_memory_resource.deallocate(mem_ptr_1, 10);
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_1, recorded_amount_1] = tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_1, -10);
  EXPECT_LT(recorded_time_1, std::chrono::system_clock::now());
  EXPECT_GT(recorded_time_1, std::chrono::system_clock::now() - std::chrono::milliseconds(100));

  auto mem_ptr_2 = std::malloc(20);
  tracking_memory_resource.deallocate(mem_ptr_2, 20);
  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 2);
  const auto& [recorded_time_2, recorded_amount_2] = tracking_memory_resource.memory_timeseries()[1];
  EXPECT_EQ(recorded_amount_2, -20);
  EXPECT_LT(recorded_time_2, std::chrono::system_clock::now());
  EXPECT_GT(recorded_time_2, std::chrono::system_clock::now() - std::chrono::milliseconds(100));
}

TEST_F(TrackingMemoryResourceTest, CreateAllocatorFromMemoryResource) {
  auto allocator = PolymorphicAllocator<int>{&tracking_memory_resource};
  auto vec = std::vector<int, PolymorphicAllocator<int>>(allocator);
  vec.reserve(5);

  ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_1, recorded_amount_1] = tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_1, sizeof(int) * 5);
  EXPECT_LT(recorded_time_1, std::chrono::system_clock::now());
  EXPECT_GT(recorded_time_1, std::chrono::system_clock::now() - std::chrono::milliseconds(100));
}

}  // namespace opossum
