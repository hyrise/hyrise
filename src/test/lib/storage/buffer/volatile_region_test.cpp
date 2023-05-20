#include <memory>

#include "base_test.hpp"

#include <filesystem>
#include "storage/buffer/volatile_region.hpp"
#include "types.hpp"

#ifdef __APPLE__
#include <mach/mach.h>
#include <sys/mman.h>
#include <sys/sysctl.h>
#include <unistd.h>
#endif

namespace hyrise {

class VolatileRegionTest : public BaseTest {};

TEST_F(VolatileRegionTest, TestAllocateDeallocate) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);

  EXPECT_EQ(volatile_region.useable_bytes(), bytes_for_size_type(size_type) * 3);

  // Allocate three
  auto frame_1 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_EQ(frame_1->data, nullptr);
  volatile_region.allocate(frame_1);
  EXPECT_NE(frame_1->data, nullptr);
  EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

  auto frame_2 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_EQ(frame_2->data, nullptr);
  volatile_region.allocate(frame_2);
  EXPECT_NE(frame_2->data, nullptr);
  EXPECT_NE(frame_1->data, frame_2->data);
  EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

  auto frame_3 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_EQ(frame_3->data, nullptr);
  volatile_region.allocate(frame_3);
  EXPECT_NE(frame_3->data, nullptr);
  EXPECT_NE(frame_3->data, frame_1->data);
  EXPECT_NE(frame_3->data, frame_2->data);
  EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

  auto invalid_frame = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_ANY_THROW(volatile_region.allocate(invalid_frame));
  EXPECT_EQ(invalid_frame->data, nullptr);

  // Deallocate two times
  volatile_region.deallocate(frame_3);
  volatile_region.deallocate(frame_1);

  // Allocate 3 times again
  auto new_frame_3 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(new_frame_3);
  EXPECT_NE(new_frame_3->data, nullptr);

  auto new_frame_1 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(new_frame_1);
  EXPECT_NE(new_frame_1->data, nullptr);

  auto new_invalid_frame = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_ANY_THROW(volatile_region.allocate(new_invalid_frame));
  EXPECT_EQ(new_invalid_frame->data, nullptr);
}

TEST_F(VolatileRegionTest, TestMove) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);
  auto frame_1 = make_frame(PageID{0}, size_type, PageType::Dram, volatile_region.mapped_memory());
  auto frame_2 = make_frame(PageID{0}, size_type, PageType::Dram);

  EXPECT_EQ(frame_1->data, volatile_region.mapped_memory());
  EXPECT_EQ(frame_2->data, nullptr);

  volatile_region.move(frame_1, frame_2);

  EXPECT_EQ(frame_2->data, volatile_region.mapped_memory());
  EXPECT_EQ(frame_1->data, nullptr);
}

TEST_F(VolatileRegionTest, TestAllocateFree) {
  auto size_type = PageSizeType::KiB32;

  auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);

  EXPECT_EQ(volatile_region.useable_bytes(), bytes_for_size_type(size_type) * 3);

  // Allocate three
  auto frame_1 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(frame_1);
  EXPECT_EQ(frame_1->size_type, size_type);
  EXPECT_NE(frame_1->data, nullptr);
  EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

  auto frame_2 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(frame_2);
  EXPECT_EQ(frame_2->size_type, size_type);
  EXPECT_NE(frame_2->data, nullptr);
  EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

  auto frame_3 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(frame_3);
  EXPECT_EQ(frame_3->size_type, size_type);
  EXPECT_NE(frame_3->data, nullptr);
  EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

  auto invalid_frame = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_ANY_THROW(volatile_region.allocate(invalid_frame));
  EXPECT_EQ(invalid_frame->data, nullptr);

  // Free two times
  volatile_region.free(frame_3);
  volatile_region.free(frame_1);

  // Allocate 3 times again
  auto new_frame_3 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(new_frame_3);
  EXPECT_NE(new_frame_3->data, nullptr);

  auto new_frame_1 = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  volatile_region.allocate(new_frame_1);
  EXPECT_NE(new_frame_1->data, nullptr);

  auto new_invalid_frame = make_frame(PageID{0}, PageSizeType::KiB32, PageType::Dram);
  EXPECT_ANY_THROW(volatile_region.allocate(new_invalid_frame));
  EXPECT_EQ(new_invalid_frame->data, nullptr);
}

TEST_F(VolatileRegionTest, TestCreateVolatileRegionsForSizeTypes) {
  {
    auto regions = create_volatile_regions_for_size_types(PageType::Dram, bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                                          NO_NUMA_MEMORY_NODE);
    for (auto i = size_t{0}; i < regions.size(); ++i) {
      EXPECT_EQ(regions[i]->get_size_type(), static_cast<PageSizeType>(i));
      EXPECT_EQ(regions[i]->get_page_type(), PageType::Dram);
      EXPECT_EQ(regions[i]->total_bytes(), bytes_for_size_type(MAX_PAGE_SIZE_TYPE));
      EXPECT_EQ(regions[i]->useable_bytes() % bytes_for_size_type(regions[i]->get_size_type()), 0);
      EXPECT_NE(regions[i]->mapped_memory(), nullptr);
    }
  }

  {
    auto regions = create_volatile_regions_for_size_types(PageType::Dram, 1.5 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                                          NO_NUMA_MEMORY_NODE);
    for (auto i = size_t{0}; i < regions.size(); ++i) {
      EXPECT_EQ(regions[i]->get_size_type(), static_cast<PageSizeType>(i));
      EXPECT_EQ(regions[i]->get_page_type(), PageType::Dram);
      EXPECT_EQ(regions[i]->total_bytes(), 96 * bytes_for_size_type(MIN_PAGE_SIZE_TYPE));
      EXPECT_EQ(regions[i]->useable_bytes() % bytes_for_size_type(regions[i]->get_size_type()), 0);
      EXPECT_NE(regions[i]->mapped_memory(), nullptr);
    }
  }

  {
    auto regions = create_volatile_regions_for_size_types(PageType::Dram, 3.2 * bytes_for_size_type(MAX_PAGE_SIZE_TYPE),
                                                          NO_NUMA_MEMORY_NODE);
    for (auto i = size_t{0}; i < regions.size(); ++i) {
      EXPECT_EQ(regions[i]->get_size_type(), static_cast<PageSizeType>(i));
      EXPECT_EQ(regions[i]->get_page_type(), PageType::Dram);
      EXPECT_EQ(regions[i]->total_bytes(), 204 * bytes_for_size_type(MIN_PAGE_SIZE_TYPE));
      EXPECT_EQ(regions[i]->useable_bytes() % bytes_for_size_type(regions[i]->get_size_type()), 0);
      EXPECT_NE(regions[i]->mapped_memory(), nullptr);
    }
  }
}

TEST_F(VolatileRegionTest, TestNumaRegionAllocateDeallocate) {
#if !HYRISE_NUMA_SUPPORT
  GTEST_SKIP() << "NUMA support not compiled in";
#endif

  auto size_type = PageSizeType::KiB32;
  const auto region_size = 1 << 30;  // 1 GiB

  auto volatile_region1 = VolatileRegion(size_type, PageType::Numa, region_size);
}

}  // namespace hyrise