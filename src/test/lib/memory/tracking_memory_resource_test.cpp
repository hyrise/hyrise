#include "base_test.hpp"
#include "memory/tracking_memory_resource.hpp"

namespace opossum {

using namespace std::chrono_literals;

class TrackingMemoryResourceTest : public BaseTest {
 protected:
  TrackingMemoryResource tracking_memory_resource;
};

TEST_F(TrackingMemoryResourceTest, PerformAllocations) {
    auto int_ptr = (int*) tracking_memory_resource.allocate(sizeof(int));
    EXPECT_NO_THROW((*int_ptr) = 1);

    auto float_ptr = (float*) tracking_memory_resource.allocate(sizeof(float)*2);
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
    EXPECT_GT(recorded_time_int, std::chrono::system_clock::now() - 100ms);

    tracking_memory_resource.allocate(sizeof(float)*2);
    ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 2);
    const auto& [recorded_time_float, recorded_amount_float] = tracking_memory_resource.memory_timeseries()[1];
    EXPECT_EQ(recorded_amount_float, sizeof(float)*2);
    EXPECT_LT(recorded_time_float, std::chrono::system_clock::now());
    EXPECT_GT(recorded_time_float, std::chrono::system_clock::now() - 100ms);
}

TEST_F(TrackingMemoryResourceTest, RecordDeallocations) {
    ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 0);

    auto mem_ptr_1 = std::malloc(10);
    tracking_memory_resource.deallocate(mem_ptr_1, 10);
    ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 1);
    const auto& [recorded_time_1, recorded_amount_1] = tracking_memory_resource.memory_timeseries()[0];
    EXPECT_EQ(recorded_amount_1, -10);
    EXPECT_LT(recorded_time_1, std::chrono::system_clock::now());
    EXPECT_GT(recorded_time_1, std::chrono::system_clock::now() - 100ms);

    auto mem_ptr_2 = std::malloc(20);
    tracking_memory_resource.deallocate(mem_ptr_2, 20);
    ASSERT_EQ(tracking_memory_resource.memory_timeseries().size(), 2);
    const auto& [recorded_time_2, recorded_amount_2] = tracking_memory_resource.memory_timeseries()[1];
    EXPECT_EQ(recorded_amount_2, -20);
    EXPECT_LT(recorded_time_2, std::chrono::system_clock::now());
    EXPECT_GT(recorded_time_2, std::chrono::system_clock::now() - 100ms);
}

}  // namespace opossum