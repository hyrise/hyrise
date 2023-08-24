#if HYRISE_NUMA_SUPPORT

#include <numa.h>
#include <numaif.h>

#endif

#include "base_test.hpp"

#include "memory/numa_memory_resource.hpp"

namespace hyrise {

class NumaMemoryResourceTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::get().storage_manager.build_memory_resources();
  }
};

#if HYRISE_NUMA_SUPPORT

// Check if data is located on target numa node.
void assert_node_location(void* buffer, NodeID target_node_id) {
  auto identified_node_idx = int32_t{};
  // Call move pages without target node id. As no target node id is set, move_pages actually sets
  // identified_node_idx to the node id, on which the buffer is residing.
  auto ret = move_pages(0, 1, reinterpret_cast<void**>(&buffer), NULL, &identified_node_idx, 0);
  ASSERT_EQ(ret, 0) << "Failed to determine the NUMA node for a given address.";
  ASSERT_EQ(target_node_id, identified_node_idx);
}

TEST_F(NumaMemoryResourceTest, AllocateDeallocate) {
  const auto num_numa_nodes = static_cast<NodeID>(Hyrise::get().topology.nodes().size());
  for (auto target_node_idx = NodeID{0}; target_node_idx < num_numa_nodes; ++target_node_idx) {
    auto resource = Hyrise::get().storage_manager.get_memory_resource(target_node_idx);
    const auto allocation_size = sizeof(uint64_t);
    auto buffer = resource->do_allocate(allocation_size, allocation_size);
    auto value = new (buffer) uint64_t{42};
    *value = 50;
    ASSERT_EQ(buffer, value);
    ASSERT_EQ(*reinterpret_cast<uint64_t*>(buffer), 50);
    assert_node_location(buffer, target_node_idx);
    resource->do_deallocate(buffer, allocation_size, allocation_size);
  }
}

TEST_F(NumaMemoryResourceTest, PolymorphicAllocator) {
  const auto num_numa_nodes = static_cast<NodeID>(Hyrise::get().topology.nodes().size());
  for (auto target_node_idx = NodeID{0}; target_node_idx < num_numa_nodes; ++target_node_idx) {
    auto resource = Hyrise::get().storage_manager.get_memory_resource(target_node_idx);
    auto allocator = PolymorphicAllocator<uint64_t>{resource};
    auto buffer = allocator.allocate(1);
    auto value = new (buffer) uint64_t{42};
    *value = 50;
    ASSERT_EQ(buffer, value);
    ASSERT_EQ(*buffer, 50);
    assert_node_location(buffer, target_node_idx);
    allocator.deallocate(buffer, 1);
  }
}

#endif

}  // namespace hyrise
