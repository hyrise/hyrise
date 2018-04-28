#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

#include "types.hpp"
#include "utils/numa_memory_resource.hpp"

namespace opossum {

int get_node_id_of(const void* ptr) {
#if HYRISE_NUMA_SUPPORT
  int status[1];
  void* addr = {const_cast<void*>(ptr)};
  numa_move_pages(0, 1, static_cast<void**>(&addr), NULL, reinterpret_cast<int*>(&status), 0);
  return status[0];
#else
  return 1;
#endif
}

class NUMAMemoryResourceTest : public BaseTest {};

TEST_F(NUMAMemoryResourceTest, BasicAllocate) {
#if HYRISE_NUMA_SUPPORT
  const int numa_node = numa_max_node();
#else
  const int numa_node = 1;
#endif

  auto memory_resource = NUMAMemoryResource(numa_node, "test");
  const auto alloc = PolymorphicAllocator<size_t>(&memory_resource);

  const auto vec = pmr_vector<size_t>(1024, alloc);

  EXPECT_EQ(get_node_id_of(vec.data()), numa_node);
}

TEST_F(NUMAMemoryResourceTest, AreEqual) {
#if HYRISE_NUMA_SUPPORT
  const int numa_node = numa_max_node();
#else
  const int numa_node = 1;
#endif

  // Two memory_resources compare equal if and only if memory allocated from one memory_resource can be deallocated
  // from the other and vice versa.

  auto resource_a = NUMAMemoryResource(numa_node, "foo");
  auto resource_b = NUMAMemoryResource(numa_node, "bar");

  EXPECT_FALSE(resource_a.is_equal(resource_b));
}

}  // namespace opossum
