#include "volatile_region.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

namespace hyrise {

VolatileRegion::VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t total_bytes,
                               const size_t numa_memory_node)
    : _frames(total_bytes / bytes_for_size_type(size_type)),
      _free_slots(_frames.size()),
      _size_type(size_type),
      _page_type(page_type),
      _numa_memory_node(numa_memory_node),
      _total_bytes(total_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
  if (_page_type == PageType::Numa && _numa_memory_node == NO_NUMA_MEMORY_NODE) {
    Fail("Cannot allocate NUMA memory without specifying a NUMA node");
  }
  _free_slots.set();
  map_memory();
}

VolatileRegion::~VolatileRegion() {
  unmap_memory();
}

// TODO: Get numa node size

void VolatileRegion::clear() {
  std::lock_guard<std::mutex> lock(_mutex);
  unmap_memory();
  map_memory();
  _free_slots.set();
}

void VolatileRegion::unmap_memory() {
  if (munmap(_mapped_memory, _total_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(errno));
  }
}

void VolatileRegion::map_memory() {
  Assert(_total_bytes != 0, "Volatile Region cannot be empty. Please increase the size of the region.");
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  _mapped_memory = static_cast<std::byte*>(mmap(NULL, _total_bytes, PROT_READ | PROT_WRITE, flags, -1, 0));
#ifdef __linux__
  madvise(_mapped_memory, _total_bytes, MADV_DONTFORK);
#endif

  if (_mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(errno));
  }

  if (_page_type == PageType::Numa) {
    to_numa(_mapped_memory);
  }
}

void VolatileRegion::free(std::shared_ptr<Frame> frame) {
  deallocate(frame);
  // https://bugs.chromium.org/p/chromium/issues/detail?id=823915
#ifdef __APPLE__
  const int flags = MADV_FREE_REUSABLE;
#elif __linux__
  const int flags = MADV_DONTNEED;
#endif
  if (madvise(frame->data, bytes_for_size_type(_size_type), flags) < 0) {
    const auto error = errno;
    Fail("Failed to madvice region: " + strerror(errno));
  }
}

void VolatileRegion::to_numa(std::byte* address) {
#if HYRISE_NUMA_SUPPORT
  Assert(_numa_memory_node != NO_NUMA_MEMORY_NODE, "Numa node has not been set.");
  numa_tonode_memory(address, _total_bytes, _numa_memory_node);
#else
  Fail("Current build does not support NUMA.");
#endif
}

void VolatileRegion::allocate(std::shared_ptr<Frame> frame) {
  std::lock_guard<std::mutex> lock(_mutex);
  DebugAssert(_free_slots.any(), "No free slots available in region.");

  auto frame_id = _free_slots.find_first();
  _free_slots.reset(frame_id);

  auto data = _mapped_memory + frame_id * bytes_for_size_type(_size_type);
  frame->data = data;

  _frames[frame_id] = frame;
}

void VolatileRegion::move(std::shared_ptr<Frame> from, std::shared_ptr<Frame> to) {
  DebugAssert(from->data >= _mapped_memory && from->data < _mapped_memory + _total_bytes,
              "Frame does not belong to this region.");

  const auto offset = reinterpret_cast<const std::byte*>(from->data) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);

  to->data = from->data;
  from->data = nullptr;
  _frames[frame_id] = to;
}

void VolatileRegion::deallocate(std::shared_ptr<Frame> frame) {
  DebugAssert(frame->page_type == _page_type, "Frame does not belong to this region.");
  DebugAssert(frame->size_type == _size_type, "Frame does not belong to this region.");
  DebugAssert(frame->data >= _mapped_memory && frame->data < _mapped_memory + _total_bytes,
              "Frame does not belong to this region.");

  // Assert(frame.use_count() == 1, "Cannot deallocate frame with active references.");

  std::lock_guard<std::mutex> lock(_mutex);
  const auto offset = reinterpret_cast<const std::byte*>(frame->data) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);
  _free_slots.set(frame_id);
}

std::shared_ptr<Frame> VolatileRegion::unswizzle(const void* ptr) {
  if (ptr < _mapped_memory || ptr >= _mapped_memory + _total_bytes) {
    return nullptr;
  }
  // Find the offset in the mapped region of the ptr and find the matching frame
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);
  return _frames[frame_id].lock();
}

size_t VolatileRegion::capacity() const {
  return _total_bytes;
}

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions_for_size_types(
    const PageType page_type, const size_t num_bytes) {
  auto array = std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>{};
  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; i++) {
    array[i] = std::make_unique<VolatileRegion>(magic_enum::enum_value<PageSizeType>(i), page_type, num_bytes);
  }
  return array;
}

}  // namespace hyrise
