#include <memory>

#include "base_test.hpp"

#include <boost/container/vector.hpp>
#include <filesystem>
#include <memory>
#include "storage/buffer/buffer_pool_allocator.hpp"
#include "storage/buffer/pin_guard.hpp"

namespace hyrise {

class PinGuardTest : public BaseTest {};

TEST_F(PinGuardTest, TestAllocatorPinGuardWithBufferPoolAllocator) {
  auto allocator = PolymorphicAllocator<int>{get_buffer_manager_memory_resource()};
  auto pin_guard = std::make_unique<AllocatorPinGuard>(allocator);
  auto vector = boost::container::vector<int, BufferPoolAllocator<int>>{2, allocator};

  auto original_frame = vector.begin().get_ptr().get_frame();
  EXPECT_TRUE(original_frame->is_pinned());

  // Resize the vector to trigger a new page allocation
  vector.resize(1000);

  auto resize_frame = vector.begin().get_ptr().get_frame();
  EXPECT_NE(original_frame, resize_frame);
  EXPECT_TRUE(resize_frame->is_pinned());

  // Trigger unpinning of page id 1
  pin_guard = nullptr;

  // No page should be pinned anymore
  EXPECT_FALSE(original_frame->is_pinned());
  EXPECT_FALSE(resize_frame->is_pinned());
}

TEST_F(PinGuardTest, TestReadPinGuard) {
  auto vector = boost::container::vector<int, BufferPoolAllocator<int>>{2};
  auto frame = vector.begin().get_ptr().get_frame();

  {
    EXPECT_FALSE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
    auto pin_guard = ReadPinGuard(vector);
    EXPECT_TRUE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
  }

  EXPECT_FALSE(frame->is_pinned());
  EXPECT_FALSE(frame->is_dirty());
}

TEST_F(PinGuardTest, TestWritePinGuard) {
  auto vector = boost::container::vector<int, BufferPoolAllocator<int>>{2};
  auto frame = vector.begin().get_ptr().get_frame();

  {
    EXPECT_FALSE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
    auto pin_guard = WritePinGuard(vector);
    EXPECT_TRUE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
  }

  EXPECT_FALSE(frame->is_pinned());
  EXPECT_TRUE(frame->is_dirty());
}

TEST_F(PinGuardTest, TestWritePinGuardWithStrings) {
  auto vector = pmr_vector<pmr_string>{2};
  auto frame = vector.begin().get_ptr().get_frame();

  {
    EXPECT_FALSE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
    auto pin_guard = WritePinGuard(vector);
    EXPECT_TRUE(frame->is_pinned());
    EXPECT_FALSE(frame->is_dirty());
  }

  EXPECT_FALSE(frame->is_pinned());
  EXPECT_TRUE(frame->is_dirty());
}
}  // namespace hyrise