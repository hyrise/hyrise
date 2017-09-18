#include <memory>

#include "../base_test.hpp"
#include "gtest/gtest.h"
#if OPOSSUM_NUMA_SUPPORT
#include <numa.h>
#endif

#include "../../lib/polymorphic_allocator.hpp"
#include "../../lib/utils/numa_memory_resource.hpp"

namespace opossum {

int get_node_id_of(const void* ptr) {
#if OPOSSUM_NUMA_SUPPORT
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
  auto memory_resource = NUMAMemoryResource(2, "test");
  const auto alloc = PolymorphicAllocator<size_t>(&memory_resource);

  const auto vec = pmr_vector<size_t>(1024, alloc);

#if OPOSSUM_NUMA_SUPPORT
  EXPECT_EQ(get_node_id_of(vec.data()), 2);
#else
  EXPECT_EQ(get_node_id_of(vec.data()), 1);
#endif
}

}  // namespace opossum
