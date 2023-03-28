#include "volatile_region.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

namespace hyrise {

VolatileRegion::VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t total_bytes,
                               const size_t memory_numa_node)
    : _frames(total_bytes / bytes_for_size_type(size_type)),
      _size_type(size_type),
      _page_type(page_type),
      _memory_numa_node(memory_numa_node),
      _total_bytes(total_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
  if (page_type == PageType::Numa && memory_numa_node == NO_NUMA_MEMORY_NODE) {
    Fail("Cannot allocate NUMA memory without specifying a NUMA node");
  }

  map_memory();
  assign_memory_to_frames();
  create_free_list();

  _free_list = &_frames[0];
}

VolatileRegion::~VolatileRegion() {
  unmap_memory();
}

// TODO: Get numa node size

void VolatileRegion::clear() {
  std::lock_guard<std::mutex> lock(_mutex);
  unmap_memory();
  map_memory();
  create_free_list();
  _free_list = &_frames[0];
}

void VolatileRegion::create_free_list() {
  for (auto frame_id = size_t{0}; frame_id < _frames.size() - 1; frame_id++) {
    _frames[frame_id].next_free_frame = &_frames[frame_id + 1];
  }
}

void VolatileRegion::unmap_memory() {
  if (munmap(_mapped_memory, _total_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(errno));
  }
}

void VolatileRegion::map_memory() {
  Assert(_total_bytes != 0, "Volatile Region cannot be empty");
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
}

void VolatileRegion::assign_memory_to_frames() {
  for (auto frame_id = size_t{0}; frame_id < _frames.size(); frame_id++) {
    _frames[frame_id].size_type = _size_type;
    _frames[frame_id].page_type = PageType::Dram;
    _frames[frame_id].data = _mapped_memory + frame_id * bytes_for_size_type(_size_type);
    if (_page_type == PageType::Numa) {
      // TODO: We might only need to do this once, but it is not worth the effort to find out right now
      to_numa(_mapped_memory);
    }
  }
}

void VolatileRegion::free(Frame* frame) {
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
  Assert(_numa_node != NO_NUMA_MEMORY_NODE, "Numa node has not been set.");
  numa_tonode_memory(address, _total_bytes, _numa_node);
#else
  Fail("Current build does not support NUMA.");
#endif
}

Frame* VolatileRegion::allocate() {
  std::lock_guard<std::mutex> lock(_mutex);
  auto frame = _free_list;
  if (frame) {
    _free_list = frame->next_free_frame;
    frame->next_free_frame = nullptr;
  }
  return frame;
}

void VolatileRegion::deallocate(Frame* frame) {
  std::lock_guard<std::mutex> lock(_mutex);
  frame->next_free_frame = _free_list;
  _free_list = frame;
}

Frame* VolatileRegion::unswizzle(const void* ptr) {
  if (ptr < _mapped_memory || ptr >= _mapped_memory + _total_bytes) {
    return nullptr;
  }
  // Find the offset in the mapped region of the ptr and find the matching frame
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);
  return &_frames[frame_id];
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
