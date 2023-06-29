#include <numa.h>
#include <numaif.h>

#include "base_test.hpp"
#include "memory/numa_memory_resource.hpp"

namespace hyrise {

class NumaMemoryResourceTest : public BaseTest {};

TEST_F(NumaMemoryResourceTest, AllocateDeallocate) {
  const auto num_numa_nodes = numa_num_configured_nodes();
  for (auto target_node_idx = NumaNodeID{0}; target_node_idx < num_numa_nodes; ++target_node_idx) {
    auto resource = NumaMemoryResource(target_node_idx);
    const auto allocation_size = sizeof(uint64_t);
    auto buffer = resource.do_allocate(allocation_size, allocation_size);
    auto value = new (buffer) uint64_t{42};
    *value = 50;
    ASSERT_EQ(buffer, value);
    ASSERT_EQ(*reinterpret_cast<uint64_t*>(buffer), 50);
    // Check if data is located on target numa node.
    auto identified_node_idx = int32_t{};
    auto ret = move_pages(0, 1, &buffer, NULL, &identified_node_idx, 0);
    ASSERT_EQ(ret, 0) << "Failed to determine the NUMA node for a given address.";
    ASSERT_EQ(target_node_idx, identified_node_idx);
    resource.do_deallocate(buffer, allocation_size, allocation_size);
  }
}

TEST_F(NumaMemoryResourceTest, PolymorphicAllocator) {
  const auto num_numa_nodes = numa_num_configured_nodes();
  for (auto target_node_idx = NumaNodeID{0}; target_node_idx < num_numa_nodes; ++target_node_idx) {
    auto resource = NumaMemoryResource(target_node_idx);
    auto allocator = PolymorphicAllocator<uint64_t>{&resource};
    auto buffer = allocator.allocate(1);
    auto value = new (buffer) uint64_t{42};
    *value = 50;
    ASSERT_EQ(buffer, value);
    ASSERT_EQ(*buffer, 50);
    // Check if data is located on target numa node.
    auto identified_node_idx = int32_t{};
    auto ret = move_pages(0, 1, reinterpret_cast<void**>(&buffer), NULL, &identified_node_idx, 0);
    ASSERT_EQ(ret, 0) << "Failed to determine the NUMA node for a given address.";
    ASSERT_EQ(target_node_idx, identified_node_idx);
    allocator.deallocate(buffer, 1);
  }
}

}  // namespace hyrise
