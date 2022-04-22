#include <memory>

#include "base_test.hpp"
#include "memory/zero_allocator.hpp"

namespace opossum {

class Chunk;

class ZeroAllocatorTest : public BaseTest {};

TEST_F(ZeroAllocatorTest, ZeroFilledSharedPointer) {
  auto allocator = ZeroAllocator<std::shared_ptr<Chunk>>{};
  auto address = allocator.allocate(0);
  EXPECT_EQ(address, nullptr);
  auto address = allocator.allocate(1024);
  EXPECT_EQ(address, nullptr);
}

}  // namespace opossum
