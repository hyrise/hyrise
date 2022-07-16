#include "base_test.hpp"
#include "hyrise.hpp"
#include "utils/memory_resource_manager.hpp"
//#include "gtest/gtest-matchers.h"
#include <gmock/gmock.h>

namespace opossum {

using namespace ::testing;

class MemoryResourceManagerTest : public BaseTest {
 protected:
  MemoryResourceManager memory_resource_manager;
};

TEST_F(MemoryResourceManagerTest, GetMemoryResources) {
  ASSERT_EQ(memory_resource_manager.memory_resources().size(), 0);

  const auto memory_resource_ptr_1 =
      memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
  ASSERT_EQ(memory_resource_manager.memory_resources().size(), 1);
  const auto resource_record_1 = memory_resource_manager.memory_resources()[0];
  EXPECT_EQ(resource_record_1.operator_type, OperatorType::Mock);
  EXPECT_EQ(resource_record_1.operator_data_structure, "my_data_structure");
  EXPECT_EQ(resource_record_1.resource_ptr, memory_resource_ptr_1);

  const auto memory_resource_ptr_2 =
      memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure_2");
  ASSERT_EQ(memory_resource_manager.memory_resources().size(), 2);
  const auto resource_record_2 = memory_resource_manager.memory_resources()[1];
  EXPECT_EQ(resource_record_2.operator_type, OperatorType::Mock);
  EXPECT_EQ(resource_record_2.operator_data_structure, "my_data_structure_2");
  EXPECT_EQ(resource_record_2.resource_ptr, memory_resource_ptr_2);
}

TEST_F(MemoryResourceManagerTest, GetMemoryResourceForSamePurposeMultipleTimes) {
  const auto memory_resource_ptr_1 =
      memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
  ASSERT_EQ(memory_resource_manager.memory_resources().size(), 1);

  const auto memory_resource_ptr_2 =
      memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure");
  ASSERT_EQ(memory_resource_manager.memory_resources().size(), 2);

  // we expect
  ASSERT_NE(memory_resource_ptr_1, memory_resource_ptr_2);
}

TEST_F(MemoryResourceManagerTest, ConcurrentCallsAreHandledCorrectly) {

  // get a memory resource, perform an allocation and a deallocation. Will be executed by multiple threads simultaneously.
  auto fetch_and_use_a_memory_resource = [&](const uint8_t indx) { 
    auto memory_resource = memory_resource_manager.get_memory_resource(OperatorType::Mock, "my_data_structure_"+std::to_string(indx));
    auto mem_ptr = memory_resource->allocate(indx+1);
    memory_resource->deallocate(mem_ptr, indx+1);
  };

  // Create a few threads that interact with the memory resource manager and obtained memory resources
  auto N_THREADS = uint8_t{10};
  auto threads = std::vector<std::thread>(N_THREADS);
  for (auto indx = uint8_t{0}; indx < N_THREADS; ++indx) {
    threads[indx] = std::thread(fetch_and_use_a_memory_resource, indx);
  }
  for (auto& thread : threads) {
    thread.join();
  }

  // Ensure that the memory resources, the allocations, and the deallcoations are recorded correctly. Each allocation or 
  // deallocation should be recorded exactly once by any memory resource.
  const auto memory_resources = memory_resource_manager.memory_resources();
  ASSERT_EQ(memory_resources.size(), N_THREADS);
  for (auto allocation_amount = uint8_t{1}; allocation_amount <= N_THREADS; ++allocation_amount ) {
    EXPECT_THAT(memory_resources, Contains(Property(&TrackingMemoryResource::memory_timeseries, Contains(Pair(_, allocation_amount)))));
    EXPECT_THAT(memory_resources, Contains(Property(&TrackingMemoryResource::memory_timeseries, Contains(Pair(_, -1 * allocation_amount)))));
  }
}

}  // namespace opossum
