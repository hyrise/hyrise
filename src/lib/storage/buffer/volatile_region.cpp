#include "volatile_region.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <boost/describe.hpp>
#include <boost/mp11.hpp>
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

// TODO: Assert page sizes with getpagesize()
namespace hyrise {

VolatileRegion::VolatileRegion(const PageSizeType size_type, const size_t total_bytes, const size_t memory_numa_node)
    : _frames(total_bytes / bytes_for_size_type(size_type)),
      _size_type(size_type),
      _memory_numa_node(memory_numa_node),
      _total_bytes(total_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
  map_memory();
  assign_memory_to_frames();
  create_free_list();

  _free_list = &_frames[0];
}

VolatileRegion::~VolatileRegion() {
  unmap_memory();
}

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
  Assert(_total_bytes != 0, "Volatile Region needs at least the size of the largest page size type");
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
    _frames[frame_id].data = _mapped_memory + frame_id * bytes_for_size_type(_size_type);
  }
}

void VolatileRegion::free(Frame* frame) {
  deallocate(frame);
#ifdef __APPLE__
  const int flags = MADV_FREE_REUSABLE;
#elif __linux__
  const int flags = MADV_DONTNEED;
#endif
  if (madvise(frame->data, bytes_for_size_type(_size_type), flags) < 0) {
    const auto error = errno;
    Fail("Failed to unmap region: " + strerror(errno));
  }
}

void VolatileRegion::to_numa(std::byte* address) {
#if HYRISE_NUMA_SUPPORT
  Assert(_numa_node != -1, "Numa node has not been set.");
  numa_tonode_memory(address, bytes_for_size_type(_size_type), _numa_node);
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
  // TODO: Try to make this branchless
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

}  // namespace hyrise
