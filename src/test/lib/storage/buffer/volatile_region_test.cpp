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

TEST_F(VolatileRegionTest, TestAllocate) {
  struct alignas(OS_PAGE_SIZE) MemoryRegion {
    std::array<std::byte, 1024 * 1024> data;
  };

  auto memory_region = MemoryRegion{};
  auto volatile_region = VolatileRegion{PageSizeType::KiB16, 1024 * 1024, memory_region.data.data()};
}

}  // namespace hyrise
