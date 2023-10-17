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
#include <fcntl.h>

namespace hyrise {

class VolatileRegionTest : public BaseTest {
 public:
  struct alignas(OS_PAGE_SIZE) MemoryRegion {
    std::array<std::byte, 1024 * 1024> data;
  };

  void SetUp() override {
    memory_region = MemoryRegion{};
  }

  MemoryRegion memory_region{};

  NodeID get_numa_node_of_page(std::byte* page) {
#if HYRISE_NUMA_SUPPORT
    int numa_node[1] = {-1};
    if (move_pages(NULL, 1, &page, NULL, numa_node, 0) < 0) {
      throw std::runtime_error("move_pages failed");
    }
    return NodeID{numa_node[0]};
#endif
    Fail("Not implemented on this platform");
  }
};

TEST_F(VolatileRegionTest, TestGetPage) {
  auto volatile_region = VolatileRegion{PageSizeType::KiB16, memory_region.data.data(),
                                        memory_region.data.data() + memory_region.data.size()};
  EXPECT_EQ(volatile_region.size(), memory_region.data.size() / bytes_for_size_type(PageSizeType::KiB16));

  EXPECT_ANY_THROW(volatile_region.get_page(PageID{PageSizeType::KiB32, 0}));

  EXPECT_EQ(volatile_region.get_page(PageID{PageSizeType::KiB16, 7}),
            memory_region.data.data() + 7 * bytes_for_size_type(PageSizeType::KiB16));
  EXPECT_EQ(volatile_region.get_page(PageID{PageSizeType::KiB16, 8}),
            memory_region.data.data() + 8 * bytes_for_size_type(PageSizeType::KiB16));
}

TEST_F(VolatileRegionTest, TestFreeAPage) {
#ifdef __APPLE__
  GTEST_SKIP() << "MADV_FREE_REUSABLE is not testable on APPLE";
#endif
  auto volatile_region = VolatileRegion{PageSizeType::KiB16, memory_region.data.data(),
                                        memory_region.data.data() + memory_region.data.size()};
  EXPECT_EQ(volatile_region.size(), memory_region.data.size() / bytes_for_size_type(PageSizeType::KiB16));

  const auto page_id = PageID{PageSizeType::KiB16, 7};
  auto page = volatile_region.get_page(page_id);
  std::memset(page, 0x1, bytes_for_size_type(PageSizeType::KiB16));
  volatile_region.free(page_id);
  std::array<std::byte, bytes_for_size_type(PageSizeType::KiB16)> zero_buffer{std::byte{0}};
  // The behaviour differs on Linux and OS X. On OS X, the page is not zeroed without memory pressue, on Linux it is.
  EXPECT_EQ(std::memcmp(page, zero_buffer.data(), bytes_for_size_type(PageSizeType::KiB16)), 0);
}

TEST_F(VolatileRegionTest, TestMbindToNumaNode) {
#if !HYRISE_NUMA_SUPPORT
  GTEST_SKIP() << "NUMA support is not enabled";
#endif
  auto volatile_region = VolatileRegion{PageSizeType::KiB16, memory_region.data.data(),
                                        memory_region.data.data() + memory_region.data.size()};
  EXPECT_EQ(volatile_region.size(), memory_region.data.size() / bytes_for_size_type(PageSizeType::KiB16));

  auto frame = volatile_region.get_frame(PageID{PageSizeType::KiB16, 7});

  EXPECT_EQ(frame->node_id(), NodeID{0});
  EXPECT_EQ(get_numa_node_of_page(volatile_region.get_page(PageID{PageSizeType::KiB16, 7})), NodeID{0});
  volatile_region.mbind_to_numa_node(PageID{PageSizeType::KiB16, 7}, NodeID{1});
  EXPECT_EQ(frame->node_id(), NodeID{1});
  EXPECT_EQ(get_numa_node_of_page(volatile_region.get_page(PageID{PageSizeType::KiB16, 7})), NodeID{1});
}

TEST_F(VolatileRegionTest, TestMovePageToNumaNode) {
#if !HYRISE_NUMA_SUPPORT
  GTEST_SKIP() << "NUMA support is not enabled";
#endif
  auto volatile_region = VolatileRegion{PageSizeType::KiB16, memory_region.data.data(),
                                        memory_region.data.data() + memory_region.data.size()};
  EXPECT_EQ(volatile_region.size(), memory_region.data.size() / bytes_for_size_type(PageSizeType::KiB16));

  auto frame = volatile_region.get_frame(PageID{PageSizeType::KiB16, 7});

  EXPECT_EQ(frame->node_id(), NodeID{0});
  // TODO: Loop all pages
  EXPECT_EQ(get_numa_node_of_page(volatile_region.get_page(PageID{PageSizeType::KiB16, 7})), NodeID{0});
  volatile_region.move_page_to_numa_node(PageID{PageSizeType::KiB16, 7}, NodeID{1});
  EXPECT_EQ(frame->node_id(), NodeID{1});
  EXPECT_EQ(get_numa_node_of_page(volatile_region.get_page(PageID{PageSizeType::KiB16, 7})), NodeID{1});
}

TEST_F(VolatileRegionTest, TestCreateVolatileRegions) {
  auto nullfd = open("/dev/random", O_WRONLY);
  auto region = VolatileRegion::create_mapped_region();
  {
    auto volatile_regions = VolatileRegion::create_volatile_regions(region);
    auto region_start = region;
    for (const auto& volatile_region : volatile_regions) {
      EXPECT_EQ(volatile_region->get_page(PageID{volatile_region->size_type(), 0}), region_start);
      EXPECT_EQ(volatile_region->size(),
                (VolatileRegion::DEFAULT_RESERVED_VIRTUAL_MEMORY / bytes_for_size_type(volatile_region->size_type())));
    }
  }
  VolatileRegion::unmap_region(region);

  // Testing a write to an invalid memory region is the only way to test if the memory region is actually unmapped.
  EXPECT_EQ(write(nullfd, region, 1024), -1) << "Writing to an invalid memory region should fail with -1";
  // TODO: This leaks the fd if the test fails
  close(nullfd);
}

}  // namespace hyrise
