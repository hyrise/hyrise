#include "volatile_region.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

namespace hyrise {

VolatileRegion::VolatileRegion(const PageSizeType size_type, std::byte* region_start, std::byte* region_end,
                               std::shared_ptr<BufferManagerMetrics> metrics)
    : _frames(std::min(INITIAL_SLOTS_PER_REGION, (region_end - region_start) / bytes_for_size_type(size_type))),
      _metrics(metrics),
      _free_slots(_frames.size()),
      _size_type(size_type),
      _region_start(region_start),
      _region_end(region_end) {
  DebugAssertPageAligned(region_start);
  DebugAssert(region_start < region_end, "Region is too small");
  DebugAssert((region_end - region_start) < DEFAULT_RESERVED_VIRTUAL_MEMORY, "Region start and end dont match");
  _free_slots.set();
  if constexpr (ENABLE_MPROTECT) {
    if (mprotect(region_start, region_end - region_start, PROT_NONE) != 0) {
      const auto error = errno;
      Fail("Failed to mprotect: " + strerror(errno));
    }
  }
}

void VolatileRegion::move_to_numa_node(PageID page_id, const NumaMemoryNode target_memory_node) {
  DebugAssert(page_id.size_type() == _size_type, "Page does not belong to this region.");

#if HYRISE_NUMA_SUPPORT
  DebugAssert(target_memory_node != NO_NUMA_MEMORY_NODE, "Numa node has not been set.");

  const auto num_bytes = bytes_for_size_type(_size_type);
  numa_tonode_memory(get_page(page_id), num_bytes, target_memory_node);
  _frames[page_id.index].set_memory_node(target_memory_node);

  if (target_memory_node == DEFAULT_DRAM_NUMA_NODE) {
    _metrics->total_bytes_copied_from_numa_to_dram.fetch_add(bytes_for_size_type(_size_type),
                                                             std::memory_order_relaxed);
  } else {
    _metrics->total_bytes_copied_from_dram_to_numa.fetch_add(bytes_for_size_type(_size_type),
                                                             std::memory_order_relaxed);
#endif
  }

  void VolatileRegion::free(PageID page_id) {
    DebugAssert(page_id.size_type() == _size_type, "Page does not belong to this region.");

    // https://bugs.chromium.org/p/chromium/issues/detail?id=823915
#ifdef __APPLE__
    const int flags = MADV_FREE_REUSABLE;
#elif __linux__
  const int flags = MADV_DONTNEED;
#endif
    const auto num_bytes = bytes_for_size_type(_size_type);
    auto ptr = get_page(page_id);
    if (madvise(ptr, num_bytes, flags) < 0) {
      const auto error = errno;
      Fail("Failed to madvice region: " + strerror(errno));
    }
    _metrics->num_madvice_free_calls.fetch_add(1, std::memory_order_relaxed);
  }

  std::pair<PageID, std::byte*> VolatileRegion::allocate() {
    // TODO: Handle missing space
    auto idx = PageID::PageIDType{0};
    {
      std::lock_guard<std::mutex> lock(_mutex);
      DebugAssert(_free_slots.any(), "No free slots available in region. TODO: Expand until end of region");
      idx = _free_slots.find_first();
      _free_slots.reset(idx);
    }

    const auto page_id = PageID{_size_type, idx};
    auto ptr = get_page(page_id);
    if constexpr (ENABLE_MPROTECT) {
      if (mprotect(ptr, bytes_for_size_type(_size_type), PROT_READ | PROT_WRITE) != 0) {
        const auto error = errno;
        Fail("Failed to mprotect: " + strerror(errno));
      }
    }
    return std::make_pair(page_id, ptr);
  }

  std::byte* VolatileRegion::get_page(PageID page_id) {
    const auto num_bytes = bytes_for_size_type(_size_type);
    auto data = _region_start + page_id.index * num_bytes;
    DebugAssertPageAligned(data);
    return data;
  }

  void VolatileRegion::deallocate(const PageID page_id) {
    DebugAssert(page_id.size_type() == _size_type, "Page does not belong to this region.");
    std::lock_guard<std::mutex> lock(_mutex);
    // TODO: Assert unlocked and clear
    _free_slots.set(page_id.index);
    if constexpr (HYRISE_DEBUG) {
      auto ptr = get_page(page_id);
      if (mprotect(ptr, bytes_for_size_type(_size_type), PROT_NONE) != 0) {
        const auto error = errno;
        Fail("Failed to mprotect: " + strerror(errno));
      }
    }
  }

  Frame* VolatileRegion::get_frame(const PageID page_id) {
    DebugAssert(page_id.size_type() == _size_type, "Page does not belong to this region.");

    return &_frames[page_id.index];
  }

  size_t VolatileRegion::memory_consumption() const {
    return sizeof(*this) + sizeof(decltype(_frames)::value_type) * _frames.capacity() +
           _free_slots.capacity() / CHAR_BIT;
  }

  void VolatileRegion::clear() {
    std::lock_guard<std::mutex> lock(_mutex);
    _free_slots.set();
    _frames.clear();
  }

}  // namespace hyrise
