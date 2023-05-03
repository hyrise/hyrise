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

class VolatileRegionTest : public BaseTest {
 public:
  size_t get_pysical_memory_usage() {
#ifdef __linux__
    std::ifstream self_status_file;
    MetaSystemUtilizationTable::ProcessMemoryUsage memory_usage{};
    try {
      self_status_file.open("/proc/self/status", std::ifstream::in);

      std::string self_status_line;
      while (std::getline(self_status_file, self_status_line)) {
        if (self_status_line.starts_with("VmRSS")) {
          return _parse_value_string(self_status_line)[0] * 1024;
        }
      }

      self_status_file.close();
    } catch (std::ios_base::failure& fail) {
      Fail("Failed to read /proc/self/status (" + fail.what() + ")");
    }

    return memory_usage;
#endif

#ifdef __APPLE__
    struct task_basic_info info {};

    mach_msg_type_number_t count = TASK_BASIC_INFO_COUNT;
    const auto ret = task_info(mach_task_self(), TASK_BASIC_INFO, reinterpret_cast<task_info_t>(&info), &count);
    Assert(ret == KERN_SUCCESS, "Failed to get task_info");

    return info.resident_size;
#endif

    Fail("Method not implemented for this platform");
  }
};

// TEST_F(VolatileRegionTest, TestAllocateDeallocate) {
//   auto size_type = PageSizeType::KiB32;

//   auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);

//   EXPECT_EQ(volatile_region.capacity(), bytes_for_size_type(size_type) * 3);

//   // Allocate three
//   auto frame_1 = volatile_region.allocate();
//   EXPECT_EQ(frame_1->size_type, size_type);
//   EXPECT_NE(frame_1->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

//   auto frame_2 = volatile_region.allocate();
//   EXPECT_EQ(frame_2->size_type, size_type);
//   EXPECT_NE(frame_2->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

//   auto frame_3 = volatile_region.allocate();
//   EXPECT_EQ(frame_3->size_type, size_type);
//   EXPECT_NE(frame_3->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

//   auto invalid_frame = volatile_region.allocate();
//   EXPECT_EQ(invalid_frame, nullptr);

//   // Deallocate two times
//   volatile_region.deallocate(frame_3);
//   volatile_region.deallocate(frame_1);

//   // Allocate 3 times again
//   auto new_frame_3 = volatile_region.allocate();
//   EXPECT_NE(new_frame_3, nullptr);
//   auto new_frame_1 = volatile_region.allocate();
//   EXPECT_NE(new_frame_1, nullptr);
//   auto new_invalid_frame = volatile_region.allocate();
//   EXPECT_EQ(new_invalid_frame, nullptr);
// }

// TEST_F(VolatileRegionTest, TestAllocateFree) {
//   auto size_type = PageSizeType::KiB32;

//   auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);

//   EXPECT_EQ(volatile_region.capacity(), bytes_for_size_type(size_type) * 3);

//   // Allocate three
//   auto frame_1 = volatile_region.allocate();
//   EXPECT_EQ(frame_1->size_type, size_type);
//   EXPECT_NE(frame_1->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_1->data, 1, bytes_for_size_type(size_type)));

//   auto frame_2 = volatile_region.allocate();
//   EXPECT_EQ(frame_2->size_type, size_type);
//   EXPECT_NE(frame_2->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_2->data, 1, bytes_for_size_type(size_type)));

//   auto frame_3 = volatile_region.allocate();
//   EXPECT_EQ(frame_3->size_type, size_type);
//   EXPECT_NE(frame_3->data, nullptr);
//   EXPECT_NO_THROW(memset(frame_3->data, 1, bytes_for_size_type(size_type)));

//   auto invalid_frame = volatile_region.allocate();
//   EXPECT_EQ(invalid_frame, nullptr);

//   // Free two times
//   volatile_region.free(frame_3);
//   volatile_region.free(frame_1);

//   // Allocate 3 times again
//   auto new_frame_3 = volatile_region.allocate();
//   EXPECT_NE(new_frame_3, nullptr);
//   auto new_frame_1 = volatile_region.allocate();
//   EXPECT_NE(new_frame_1, nullptr);
//   auto new_invalid_frame = volatile_region.allocate();
//   EXPECT_EQ(new_invalid_frame, nullptr);
// }

// TEST_F(VolatileRegionTest, TestUnswizzle) {
//   auto size_type = PageSizeType::KiB32;

//   auto volatile_region = VolatileRegion(size_type, PageType::Dram, bytes_for_size_type(size_type) * 3 + 10);
//   auto frame_1 = volatile_region.allocate();
//   auto frame_2 = volatile_region.allocate();
//   auto frame_3 = volatile_region.allocate();

//   EXPECT_EQ(volatile_region.unswizzle(frame_1->data + 10), frame_1);
//   EXPECT_EQ(volatile_region.unswizzle(frame_3->data + 512), frame_3);
//   EXPECT_EQ(volatile_region.unswizzle(frame_2->data + 25), frame_2);
// }

// TEST_F(VolatileRegionTest, TestFreeReducesPhysicalMemoryUsage) {
//   auto size_type = PageSizeType::KiB32;
//   const auto region_size = 1 << 30;  // 1 GiB

//   auto volatile_region1 = VolatileRegion(size_type, PageType::Dram, region_size);
//   auto volatile_region2 = VolatileRegion(size_type, PageType::Dram, region_size);

//   const auto num_allocations = 32000;

//   std::cout << get_pysical_memory_usage() << std::endl;

//   std::vector<Frame*> allocations1;
//   for (auto i = size_t{0}; i < num_allocations; ++i) {
//     allocations1.push_back(volatile_region1.allocate());
//   }

//   std::cout << get_pysical_memory_usage() << std::endl;

//   for (auto frame : allocations1) {
//     memset(frame->data, 1, bytes_for_size_type(size_type));
//   }

//   std::cout << get_pysical_memory_usage() << std::endl;

//   for (auto frame : allocations1) {
//     volatile_region1.free(frame);
//   }

//   std::cout << get_pysical_memory_usage() << std::endl;

//   sleep(2);

//   std::vector<Frame*> allocations2;
//   for (auto i = size_t{0}; i < num_allocations; ++i) {
//     allocations2.push_back(volatile_region2.allocate());
//   }
//   std::cout << get_pysical_memory_usage() << std::endl;

//   sleep(2);

//   for (auto frame : allocations2) {
//     memset(frame->data, 1, bytes_for_size_type(size_type));
//   }

//   sleep(2);

//   std::cout << get_pysical_memory_usage() << std::endl;

//   for (auto frame : allocations2) {
//     volatile_region2.free(frame);
//   }
// }

TEST_F(VolatileRegionTest, TestNumaRegionAllocateDeallocate) {
#if !HYRISE_NUMA_SUPPORT
  GTEST_SKIP();
#endif

  auto size_type = PageSizeType::KiB32;
  const auto region_size = 1 << 30;  // 1 GiB

  auto volatile_region1 = VolatileRegion(size_type, PageType::Numa, region_size);
}

}  // namespace hyrise