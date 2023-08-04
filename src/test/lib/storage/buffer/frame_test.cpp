#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/frame.hpp"
#include "storage/buffer/helper.hpp"
#include "types.hpp"

std::size_t _delete_count = 0;

void operator delete(void* p) noexcept {
  // _delete_count++;
  std::free(p);
}

namespace hyrise {

class FramePtrTest : public BaseTest {
  static_assert(sizeof(FramePtr) == 8);
  // static_assert(sizeof(Frame) == 120);
};

TEST_F(FramePtrTest, TestFramePtrRefCounting) {
  _delete_count = 0;
  auto frame = new Frame{PageID{4}, PageSizeType::KiB8, PageType::Dram};
  EXPECT_EQ(frame->_internal_ref_count(), 0);

  auto frame_ptr1 = FramePtr(frame);
  EXPECT_EQ(frame->_internal_ref_count(), 1);

  auto frame_ptr2 = FramePtr(frame);
  EXPECT_EQ(frame->_internal_ref_count(), 2);

  frame_ptr2 = nullptr;
  EXPECT_EQ(frame->_internal_ref_count(), 1);

  EXPECT_EQ(_delete_count, 0);
  frame_ptr1 = nullptr;
  EXPECT_EQ(_delete_count, 1);
}

TEST_F(FramePtrTest, TestFramePtrAvoidingCircularDependency) {
  _delete_count = 0;

  auto dram_frame = make_frame(PageID{4}, PageSizeType::KiB8, PageType::Dram);
  auto numa_frame = make_frame(PageID{4}, PageSizeType::KiB8, PageType::Numa);

  EXPECT_EQ(dram_frame->_internal_ref_count(), 1);
  EXPECT_EQ(numa_frame->_internal_ref_count(), 1);

  // Link both frame together to create a circular dependency
  dram_frame->sibling_frame = numa_frame;
  numa_frame->sibling_frame = dram_frame;

  EXPECT_EQ(dram_frame->_internal_ref_count(), 2);
  EXPECT_EQ(numa_frame->_internal_ref_count(), 2);

  // Delete one of the frame ptrs and verify that the circular dependency is broken up
  dram_frame = nullptr;
  EXPECT_EQ(_delete_count, 0);
  EXPECT_EQ(numa_frame->_internal_ref_count(), 1);
  EXPECT_EQ(numa_frame->sibling_frame->page_type, PageType::Dram);
  EXPECT_EQ(numa_frame->sibling_frame->sibling_frame, nullptr);
  EXPECT_EQ(numa_frame->sibling_frame->_internal_ref_count(), 1);
  EXPECT_EQ(numa_frame->_internal_ref_count(), 1);

  numa_frame = nullptr;
  EXPECT_EQ(_delete_count, 2);
}

}  // namespace hyrise
