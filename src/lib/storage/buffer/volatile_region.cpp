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

VolatileRegion::VolatileRegion(const PageSizeType size_type, const size_t total_bytes)
    : _frames(total_bytes / bytes_for_size_type(size_type)),
      _size_type(size_type),
      _total_bytes(total_bytes / bytes_for_size_type(size_type) * bytes_for_size_type(size_type)) {
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

  for (auto frame_id = size_t{0}; frame_id < _frames.size() - 1; frame_id++) {
    _frames[frame_id].next_free_frame = &_frames[frame_id + 1];
    _frames[frame_id].size_type = size_type;
    _frames[frame_id].data = _mapped_memory + frame_id * bytes_for_size_type(size_type);
  }
  _frames.back().size_type = size_type;
  _frames.back().data = _mapped_memory + (_frames.size() - 1) * bytes_for_size_type(size_type);

  _free_list = &_frames[0];

  // Assign an equal share of memory to each of the size types. We start with the biggest chunk type. If there is some bytes left, there are given to the next size type.
  // By setting the free lists elements, the pages are actually allocated through the virtual memory area.
  // auto bytes_left_from_previous = 0;
  // boost::mp11::mp_for_each<boost::mp11::mp_reverse<boost::describe::describe_enumerators<PageSizeType>>>(
  //     [&](auto page_size_type) {
  //       const auto region_index = static_cast<size_t>(page_size_type.value);
  //       _regions[region_index] = _mapped_memory + region_index * _total_bytes;
  //       const auto page_size_in_bytes = bytes_for_size_type(page_size_type.value);

  //       // We get a fair share of bytes + the rest of bytes that was not assignable to the larger region
  //       auto bytes_left_in_region = (_total_bytes / NUM_PAGE_SIZE_TYPES) + bytes_left_from_previous;
  //       for (auto page_idx = size_t{0}; bytes_left_in_region >= page_size_in_bytes;
  //            page_idx++, bytes_left_in_region -= page_size_in_bytes) {
  //         auto page = reinterpret_cast<std::byte**>(_regions[region_index] + page_idx * page_size_in_bytes);
  //         *page = _regions[region_index] + (page_idx + 1) * page_size_in_bytes;
  //       }
  //       _free_list_per_region[region_index] = _regions[region_index];
  //       bytes_left_from_previous = bytes_left_in_region;
  //     });
}

VolatileRegion::~VolatileRegion() {
  if (munmap(_mapped_memory, _total_bytes) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(errno));
  }
}

void VolatileRegion::free(Frame* frame) {
  deallocate(frame);
#ifdef __APPLE__
  const int flags = MADV_FREE;
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
  // TODO: Try to make this banchless
  if (ptr < _mapped_memory || ptr >= _mapped_memory + _total_bytes) {
    return nullptr;
  }
  // Find the offset in the mapped region of the ptr and find the matching frame
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _mapped_memory;
  const auto frame_id = offset / bytes_for_size_type(_size_type);
  return &_frames[frame_id];
}

// FrameID VolatileRegion::to_frame_id(const void* ptr) const {
//   // TODO: This could be made branchless using a mask
//   if (ptr < _regions.data() || ptr >= _regions.data() + _total_bytes * _regions.size()) {
//     return INVALID_FRAME_ID;
//   }
//   // const auto offset = reinterpret_cast<const std::byte*>(ptr) - reinterpret_cast<const std::byte*>(_frames);
//   // return FrameID{offset / bytes_for_size_type(_size_type)};
// }

// std::byte* VolatileRegion::get_page(const FrameID frame_id) {
//   DebugAssert(frame_id < capacity(), "Cannot request a frame id larger than capacity.");
//   return 0;  //_frames + frame_id;  // * bytes_for_size_type(_size_type);
// }

// void VolatileRegion::deallocate(const PageSizeType size_type, std::byte* ptr) {
//   const auto region_index = static_cast<size_t>(size_type);
//   const auto page_size_in_bytes = bytes_for_size_type(size_type);

//   DebugAssert(ptr >= _regions[region_index], "Ptr should be in region.");
//   DebugAssert(ptr < _regions[region_index] + _total_bytes, "Ptr should be in region.");

//   auto old_free_list = _free_list_per_region[region_index];
//   auto free_list_cast = reinterpret_cast<std::byte**>(ptr);
//   *free_list_cast = old_free_list;
//   _free_list_per_region[region_index] = ptr;
//   _used_bytes -= page_size_in_bytes;
// }
}  // namespace hyrise
