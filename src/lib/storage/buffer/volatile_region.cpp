#include "volatile_region.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include "utils/assert.hpp"

#if HYRISE_NUMA_SUPPORT
#include <numa.h>
#endif

namespace hyrise {

VolatileRegion::VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t num_bytes,
                               const int8_t numa_memory_node)
    : _frames(num_bytes / bytes_for_size_type(size_type)),
      _free_slots(_frames.size()),
      _size_type(size_type),
      _page_type(page_type),
      _total_bytes(num_bytes),
      _useable_bytes(num_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
  // TODO: Imrpove for emulation mode
  // if (_page_type == PageType::Numa && _numa_memory_node == NO_NUMA_MEMORY_NODE) {
  //   Fail("Cannot allocate NUMA memory without specifying a NUMA node");
  // }
  _free_slots.set();
  _mapped_memory = map_memory(_total_bytes, numa_memory_node);
}

VolatileRegion::VolatileRegion(const PageSizeType size_type, const PageType page_type, const size_t num_bytes,
                               std::byte* premapped_region)
    : _frames(num_bytes / bytes_for_size_type(size_type)),
      _free_slots(_frames.size()),
      _size_type(size_type),
      _page_type(page_type),
      _total_bytes(num_bytes),
      _mapped_memory(premapped_region),
      _useable_bytes(num_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
  _free_slots.set();
}

// TODO: Get numa node size

void VolatileRegion::unmap_memory(std::byte* memory, const size_t num_bytes) {
  if (munmap(memory, num_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(errno));
  }
}

std::byte* VolatileRegion::map_memory(const size_t num_bytes, const int8_t numa_memory_node) {
  Assert(num_bytes != 0, "Volatile Region cannot be empty. Please increase the size of the region.");
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  auto mapped_memory = static_cast<std::byte*>(mmap(NULL, num_bytes, PROT_READ | PROT_WRITE, flags, -1, 0));
#ifdef __linux__
  madvise(_mapped_memory, num_bytes, MADV_DONTFORK);
#endif

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(errno));
  }

  if (numa_memory_node != NO_NUMA_MEMORY_NODE) {
#if HYRISE_NUMA_SUPPORT
    Assert(numa_memory_node != NO_NUMA_MEMORY_NODE, "Numa node has not been set.");
    numa_tonode_memory(mapped_memory, num_bytes, numa_memory_node);
#else
    Fail("Current build does not support NUMA.");
#endif
  }
  return mapped_memory;
}

void VolatileRegion::free(FramePtr frame) {
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

void VolatileRegion::allocate(FramePtr frame) {
  std::lock_guard<std::mutex> lock(_mutex);
  DebugAssert(_free_slots.any(), "No free slots available in region.");
  DebugAssert(frame->page_type == _page_type, "Frame does not belong to this region.");
  DebugAssert(frame->size_type == _size_type, "Frame does not belong to this region.");

  auto frame_id = _free_slots.find_first();
  _free_slots.reset(frame_id);

  auto data = _mapped_memory + frame_id * bytes_for_size_type(_size_type);
  frame->data = data;

  _frames[frame_id] = frame.get();
}

void VolatileRegion::move(FramePtr from, FramePtr to) {
  std::lock_guard<std::mutex> lock(_mutex);

  DebugAssert(from->data >= _mapped_memory && from->data < _mapped_memory + _total_bytes,
              "Frame does not belong to this region.");

  const auto offset = reinterpret_cast<const std::byte*>(from->data) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);

  to->data = from->data;
  from->data = nullptr;
  _frames[frame_id] = to.get();
}

void VolatileRegion::deallocate(FramePtr frame) {
  std::lock_guard<std::mutex> lock(_mutex);
  DebugAssert(frame->page_type == _page_type, "Frame does not belong to this region.");
  DebugAssert(frame->size_type == _size_type, "Frame does not belong to this region.");
  DebugAssert(frame->data >= _mapped_memory && frame->data < _mapped_memory + _total_bytes,
              "Frame does not belong to this region.");

  // Assert(frame.use_count() == 1, "Cannot deallocate frame with active references.");

  const auto offset = reinterpret_cast<const std::byte*>(frame->data) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);
  _frames[frame_id] = nullptr;
  _free_slots.set(frame_id);
}

Frame* VolatileRegion::frame_at_index(const size_t index) const {
  return _frames[index];
}

std::byte* VolatileRegion::mapped_memory() const {
  return _mapped_memory;
}

size_t VolatileRegion::useable_bytes() const {
  return _useable_bytes;
}

size_t VolatileRegion::total_bytes() const {
  return _total_bytes;
}

size_t VolatileRegion::memory_consumption() const {
  return sizeof(*this) + sizeof(decltype(_frames)::value_type) * _frames.capacity() + _free_slots.capacity() / CHAR_BIT;
}

void VolatileRegion::clear() {
  std::lock_guard<std::mutex> lock(_mutex);
  _free_slots.set();
  _frames.clear();
}

PageType VolatileRegion::get_page_type() const {
  return _page_type;
}

PageSizeType VolatileRegion::get_size_type() const {
  return _size_type;
}

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions_for_size_types(
    const PageType page_type, const size_t num_bytes, const int8_t numa_memory_node) {
  auto array = std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>{};

  // Ensure that all regions are adjacent in memory by creating one large region and giving each size type a slice of it
  const auto fitted_num_bytes =
      num_bytes / bytes_for_size_type(MIN_PAGE_SIZE_TYPE) * bytes_for_size_type(MIN_PAGE_SIZE_TYPE);
  const auto mapped_region = VolatileRegion::map_memory(fitted_num_bytes * NUM_PAGE_SIZE_TYPES, numa_memory_node);

  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; i++) {
    array[i] = std::make_unique<VolatileRegion>(magic_enum::enum_value<PageSizeType>(i), page_type, fitted_num_bytes,
                                                mapped_region + fitted_num_bytes * i);
  }

  return array;
}

}  // namespace hyrise
