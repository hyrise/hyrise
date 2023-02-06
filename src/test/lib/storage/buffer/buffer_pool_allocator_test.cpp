#include <memory>

#include "base_test.hpp"

#include <boost/container/vector.hpp>
#include <filesystem>
#include "storage/buffer/buffer_pool_allocator.hpp"

namespace hyrise {

class BufferPoolAllocatorTest : public BaseTest {};

TEST_F(BufferPoolAllocatorTest, TestAllocateVector) {
  // BufferPoolResource uses the global Hyrise Buffer Manager
  auto allocator = BufferPoolAllocator<size_t>();

  auto data = boost::container::vector<int, BufferPoolAllocator<int>>{5, allocator};
  data[0] = 1;
  data[1] = 2;
  data[2] = 3;
  data[3] = 4;
  data[4] = 5;

  auto page = Hyrise::get().buffer_manager.get_page(PageID{0});
  auto raw_data = reinterpret_cast<int*>(page->data.data());
  EXPECT_EQ(raw_data[0], 1);
  EXPECT_EQ(raw_data[1], 2);
  EXPECT_EQ(raw_data[2], 3);
  EXPECT_EQ(raw_data[3], 4);
  EXPECT_EQ(raw_data[4], 5);
}

}  // namespace hyrise