#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/volatile_region.hpp"
#include "types.hpp"

namespace hyrise {

class VolatileRegionTest : public BaseTest {};

TEST_F(VolatileRegionTest, TestAllocateDeallocate) {
  auto volatile_region = VolatileRegion(sizeof(Page32KiB) * 3);

  EXPECT_EQ(volatile_region.capacity(), 3);
  EXPECT_EQ(volatile_region.size(), 0);

  // Allocate three times
  auto [frame_id_0, page_0] = volatile_region.allocate();
  auto [frame_id_1, page_1] = volatile_region.allocate();
  auto [frame_id_2, page_2] = volatile_region.allocate();
  auto [invalid_frame_id, invalid_page] = volatile_region.allocate();
  EXPECT_EQ(volatile_region.size(), 3);
  EXPECT_EQ(frame_id_0, 0);
  EXPECT_EQ(frame_id_1, 1);
  EXPECT_EQ(frame_id_2, 2);
  EXPECT_EQ(invalid_frame_id, INVALID_FRAME_ID);

  // Deallocate two times
  volatile_region.deallocate(frame_id_0);
  volatile_region.deallocate(frame_id_2);
  EXPECT_EQ(volatile_region.size(), 1);

  // Allocate two times again
  auto [new_frame_id_2, new_page_2] = volatile_region.allocate();
  auto [new_frame_id_0, new_page_1] = volatile_region.allocate();
  auto [new_invalid_frame_id, new_invalid_page] = volatile_region.allocate();
  EXPECT_EQ(volatile_region.size(), 3);
  EXPECT_EQ(new_frame_id_2, 2);
  EXPECT_EQ(new_frame_id_0, 0);
  EXPECT_EQ(new_invalid_frame_id, INVALID_FRAME_ID);
}

TEST_F(VolatileRegionTest, TestGetPage) {
  auto volatile_region = VolatileRegion(sizeof(Page32KiB) * 3);
  auto [frame_id, page_0] = volatile_region.allocate();
  EXPECT_EQ(volatile_region.get_page(FrameID{0}), page_0);
  EXPECT_EQ(volatile_region.get_page(FrameID{1}), page_0 + 1);
  EXPECT_EQ(volatile_region.get_page(FrameID{2}), page_0 + 2);
  EXPECT_ANY_THROW(volatile_region.get_page(FrameID{3}));
}

TEST_F(VolatileRegionTest, TestGetFrameIDFromPtr) {
  auto volatile_region = VolatileRegion(sizeof(Page32KiB) * 2);
  auto [frame_id, page_0] = volatile_region.allocate();
  EXPECT_EQ(volatile_region.get_frame_id_from_ptr(page_0 - 1), INVALID_FRAME_ID);
  EXPECT_EQ(volatile_region.get_frame_id_from_ptr(page_0), 0);
  EXPECT_EQ(volatile_region.get_frame_id_from_ptr(page_0 + 1), 1);
  EXPECT_EQ(volatile_region.get_frame_id_from_ptr(page_0 + 2), INVALID_FRAME_ID);
}

}  // namespace hyrise