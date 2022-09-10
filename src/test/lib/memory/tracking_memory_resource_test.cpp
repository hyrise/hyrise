#include "base_test.hpp"
#include "memory/tracking_memory_resource.hpp"

namespace opossum {

class TrackingMemoryResourceTest : public BaseTest {
 protected:
  TrackingMemoryResource _tracking_memory_resource;
};

TEST_F(TrackingMemoryResourceTest, RecordAllocations) {
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 0);

  _tracking_memory_resource.allocate(sizeof(int8_t));
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_int, recorded_amount_int] = _tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_int, sizeof(int8_t));
  EXPECT_LT(recorded_time_int, std::chrono::steady_clock::now());
  EXPECT_GT(recorded_time_int, std::chrono::steady_clock::now() - std::chrono::milliseconds(100));

  _tracking_memory_resource.allocate(sizeof(float) * 2);
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 2);
  const auto& [recorded_time_float, recorded_amount_float] = _tracking_memory_resource.memory_timeseries()[1];
  EXPECT_EQ(recorded_amount_float, sizeof(float) * 2);
  EXPECT_LT(recorded_time_float, std::chrono::steady_clock::now());
  EXPECT_GT(recorded_time_float, std::chrono::steady_clock::now() - std::chrono::milliseconds(100));
}

TEST_F(TrackingMemoryResourceTest, RecordDeallocations) {
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 0);

  const auto allocated_memory_pointer_1 = std::malloc(10);
  _tracking_memory_resource.deallocate(allocated_memory_pointer_1, 10);
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_1, recorded_amount_1] = _tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_1, -10);
  EXPECT_LT(recorded_time_1, std::chrono::steady_clock::now());
  EXPECT_GT(recorded_time_1, std::chrono::steady_clock::now() - std::chrono::milliseconds(100));

  const auto allocated_memory_pointer_2 = std::malloc(20);
  _tracking_memory_resource.deallocate(allocated_memory_pointer_2, 20);
  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 2);
  const auto& [recorded_time_2, recorded_amount_2] = _tracking_memory_resource.memory_timeseries()[1];
  EXPECT_EQ(recorded_amount_2, -20);
  EXPECT_LT(recorded_time_2, std::chrono::steady_clock::now());
  EXPECT_GT(recorded_time_2, std::chrono::steady_clock::now() - std::chrono::milliseconds(100));
}

TEST_F(TrackingMemoryResourceTest, CreateAllocatorFromMemoryResource) {
  auto allocator = PolymorphicAllocator<int8_t>{&_tracking_memory_resource};
  auto vec = std::vector<int8_t, PolymorphicAllocator<int8_t>>(allocator);
  vec.reserve(5);

  ASSERT_EQ(_tracking_memory_resource.memory_timeseries().size(), 1);
  const auto& [recorded_time_1, recorded_amount_1] = _tracking_memory_resource.memory_timeseries()[0];
  EXPECT_EQ(recorded_amount_1, sizeof(int8_t) * 5);
  EXPECT_LT(recorded_time_1, std::chrono::steady_clock::now());
  EXPECT_GT(recorded_time_1, std::chrono::steady_clock::now() - std::chrono::milliseconds(100));
}

}  // namespace opossum
