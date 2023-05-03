#include <memory>

#include "base_test.hpp"

#include <boost/container/vector.hpp>
#include <filesystem>
#include <memory>
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "storage/buffer/pin_guard.hpp"

namespace hyrise {

class PinGuardTest : public BaseTest {
 public:
};

TEST_F(PinGuardTest, TestAllocatorPinGuardWithBufferPoolAllocator) {
  // // Note: This mechanism only seems to work properly with the global buffer manager due to conversion between raw pointer etc
  // auto& buffer_manager = BufferManager::get_global_buffer_manager();

  // auto allocator = BufferPoolAllocator<int>{&buffer_manager};
  // auto pin_guard = std::make_unique<AllocatorPinGuard<BufferPoolAllocator<int>>>(allocator);
  // auto vector = boost::container::vector<int, BufferPoolAllocator<int>>{100, allocator};

  // // The page id 0 should be pinned
  // EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{0});
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 1);

  // // Resize the vector to trigger a new page allocation
  // vector.resize(1000);

  // //The page id 0 should not be pinned anymore, but page id 1 should be pinned
  // EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{1});
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 1);

  // // Trigger unpinning of page id 1
  // pin_guard = nullptr;

  // // No page should be pinned anymore
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 0);
}

TEST_F(PinGuardTest, TestAllocatorPinGuardWithPolymorphicAllocator) {
  // // Note: This mechanism only seems to work properly with the global buffer manager due to conversion between raw pointer etc
  // auto& buffer_manager = BufferManager::get_global_buffer_manager();

  // auto allocator = PolymorphicAllocator<int>{&buffer_manager};
  // auto pin_guard = std::make_unique<AllocatorPinGuard<PolymorphicAllocator<int>>>(allocator);
  // auto vector = pmr_vector<int>{100, allocator};

  // // The page id 0 should be pinned
  // EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{0});
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 1);

  // // Resize the vector to trigger a new page allocation
  // vector.resize(1000);

  // //The page id 0 should not be pinned anymore, but page id 1 should be pinned
  // EXPECT_EQ(vector.begin().get_ptr().get_page_id(), PageID{1});
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 1);

  // // Trigger unpinning of page id 1
  // pin_guard = nullptr;

  // // No page should be pinned anymore
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{0}), 0);
  // EXPECT_EQ(get_pin_count(buffer_manager, PageID{1}), 0);
}
}  // namespace hyrise