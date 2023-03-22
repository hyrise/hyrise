#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/volatile_region.hpp"
#include "types.hpp"

namespace hyrise {

class VolatileRegionTest : public BaseTest {};

TEST_F(VolatileRegionTest, TestAllocateDeallocate) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, bytes_for_size_type(size_type) * 3 + 10);

  EXPECT_EQ(volatile_region.capacity(), bytes_for_size_type(size_type) * 3);

  // Allocate three
  auto frame_1 = volatile_region.allocate();
  EXPECT_EQ(frame_1->size_type, size_type);
  EXPECT_NE(frame_1->data, nullptr);
  EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

  auto frame_2 = volatile_region.allocate();
  EXPECT_EQ(frame_2->size_type, size_type);
  EXPECT_NE(frame_2->data, nullptr);
  EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

  auto frame_3 = volatile_region.allocate();
  EXPECT_EQ(frame_3->size_type, size_type);
  EXPECT_NE(frame_3->data, nullptr);
  EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

  auto invalid_frame = volatile_region.allocate();
  EXPECT_EQ(invalid_frame, nullptr);

  // Deallocate two times
  volatile_region.deallocate(frame_3);
  volatile_region.deallocate(frame_1);

  // Allocate 3 times again
  auto new_frame_3 = volatile_region.allocate();
  EXPECT_NE(new_frame_3, nullptr);
  auto new_frame_1 = volatile_region.allocate();
  EXPECT_NE(new_frame_1, nullptr);
  auto new_invalid_frame = volatile_region.allocate();
  EXPECT_EQ(new_invalid_frame, nullptr);
}

TEST_F(VolatileRegionTest, TestAllocateFree) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, bytes_for_size_type(size_type) * 3 + 10);

  EXPECT_EQ(volatile_region.capacity(), bytes_for_size_type(size_type) * 3);

  // Allocate three
  auto frame_1 = volatile_region.allocate();
  EXPECT_EQ(frame_1->size_type, size_type);
  EXPECT_NE(frame_1->data, nullptr);
  EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

  auto frame_2 = volatile_region.allocate();
  EXPECT_EQ(frame_2->size_type, size_type);
  EXPECT_NE(frame_2->data, nullptr);
  EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

  auto frame_3 = volatile_region.allocate();
  EXPECT_EQ(frame_3->size_type, size_type);
  EXPECT_NE(frame_3->data, nullptr);
  EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

  auto invalid_frame = volatile_region.allocate();
  EXPECT_EQ(invalid_frame, nullptr);

  // Free two times
  volatile_region.free(frame_3);
  volatile_region.free(frame_1);

  // Allocate 3 times again
  auto new_frame_3 = volatile_region.allocate();
  EXPECT_NE(new_frame_3, nullptr);
  auto new_frame_1 = volatile_region.allocate();
  EXPECT_NE(new_frame_1, nullptr);
  auto new_invalid_frame = volatile_region.allocate();
  EXPECT_EQ(new_invalid_frame, nullptr);
}

TEST_F(VolatileRegionTest, TestUnswizzle) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, bytes_for_size_type(size_type) * 3 + 10);
  auto frame_1 = volatile_region.allocate();
  auto frame_2 = volatile_region.allocate();
  auto frame_3 = volatile_region.allocate();

  EXPECT_EQ(volatile_region.unswizzle(frame_1->data + 10), frame_1);
  EXPECT_EQ(volatile_region.unswizzle(frame_3->data + 512), frame_3);
  EXPECT_EQ(volatile_region.unswizzle(frame_2->data + 25), frame_2);
}

}  // namespace hyrise