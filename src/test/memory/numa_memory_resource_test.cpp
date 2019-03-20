#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"
#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

#include "memory/numa_memory_resource.hpp"
#include "types.hpp"

namespace opossum {

int get_node_id_of(const void* ptr) {
#if HYRISE_NUMA_SUPPORT
  int status[1];
  void* addr = {const_cast<void*>(ptr)};
  numa_move_pages(0, 1, static_cast<void**>(&addr), nullptr, reinterpret_cast<int*>(&status), 0);
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

}  // namespace opossum
