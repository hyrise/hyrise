#include "storage/buffer/helper.hpp"
#include "storage/buffer/buffer_manager.hpp"
#include "storage/buffer/volatile_region.hpp"

#include <sys/mman.h>
#include <unistd.h>
#include <chrono>
#include <fstream>
#include <utility>

namespace hyrise {

bool EvictionItem::can_evict(Frame::StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::MARKED && Frame::version(state_and_version) == timestamp;
}

bool EvictionItem::can_mark(Frame::StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::UNLOCKED && Frame::version(state_and_version) == timestamp;
}

boost::container::pmr::memory_resource* get_buffer_manager_memory_resource() {
  return &BufferManager::get();
}

//----------------------------------------------------
// Helper Functions for Memory Mapping and Yielding
//----------------------------------------------------

std::byte* create_mapped_region() {
  Assert(bytes_for_size_type(MIN_PAGE_SIZE_TYPE) >= get_os_page_size(),
         "Smallest page size does not fit into an OS page: " + std::to_string(get_os_page_size()));
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  const auto mapped_memory =
      static_cast<std::byte*>(mmap(NULL, DEFAULT_RESERVED_VIRTUAL_MEMORY, PROT_READ | PROT_WRITE, flags, -1, 0));

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(error));
  }

  return mapped_memory;
}

std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(
    std::byte* mapped_region, std::shared_ptr<BufferManagerMetrics> metrics) {
  DebugAssert(mapped_region != nullptr, "Region not properly mapped");
  auto array = std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>{};

  // Ensure that every region has the same amount of virtual memory
  // Round to the next multiple of the largest page size
  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; i++) {
    array[i] = std::make_shared<VolatileRegion>(
        magic_enum::enum_value<PageSizeType>(i), mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * i,
        mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * (i + 1), metrics);
  }

  return array;
}

void unmap_region(std::byte* region) {
  if (munmap(region, DEFAULT_RESERVED_VIRTUAL_MEMORY) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(error));
  }
}
}  // namespace hyrise