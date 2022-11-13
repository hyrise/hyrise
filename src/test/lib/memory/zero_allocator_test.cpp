#include <memory>

#include "base_test.hpp"
#include "memory/zero_allocator.hpp"

namespace hyrise {

class Chunk;

class ZeroAllocatorTest : public BaseTest {};

TEST_F(ZeroAllocatorTest, ZeroFilledSharedPointer) {
  auto allocator = ZeroAllocator<std::shared_ptr<Chunk>>{};
  EXPECT_EQ(*allocator.allocate(1), nullptr);
  const auto allocation_count = 500;
  auto ptr = allocator.allocate(allocation_count);
  for (auto allocation_index = size_t{0}; allocation_index < allocation_count; ++allocation_index) {
    EXPECT_EQ(*(ptr + allocation_index), nullptr);
  }
}

}  // namespace hyrise
