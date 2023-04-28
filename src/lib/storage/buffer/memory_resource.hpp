#pragma once

#include <forward_list>
#include "storage/buffer/types.hpp"

namespace hyrise {

template <typename T>
class BufferPtr;
class BufferManager;

class MemoryResource {
 public:
  virtual ~MemoryResource() = default;
  virtual BufferPtr<void> allocate(const std::size_t bytes, const std::size_t alignment) = 0;
  virtual void deallocate(BufferPtr<void> p, const std::size_t bytes, const std::size_t alignment) = 0;
};

/**
 * The MonotonicBufferResource is a memory resource that allocates memory in pages without deallocation. Its supposed to be fast and helpful to reduce internal fragmentation for small allocations in pages when using the buffer manager. 
 * It was designed to reduce the memory usage of pmr_string from a pmr_vector.
 * 
 * If the allocated bytes fill up more than 80% of a potential page size type, a new page is allocated. The existing current page is not touched and kept for the next allocation.
 * 
 * Inspired by std::pmr::monotonic_buffer_resource and adapted some code from boost::container::pmr::monotonic_buffer_resource. 
 * TODO: Verify usage for shared_frame (maybe pin), Use 256 KB as upper limit for page size, use 8 KB as lower limit for page size
*/
class MonotonicBufferResource : public MemoryResource {
 public:
  MonotonicBufferResource();
  MonotonicBufferResource(MemoryResource* memory_resource,
                          const std::size_t initial_size = bytes_for_size_type(PageSizeType::KiB8));

  BufferPtr<void> allocate(std::size_t bytes, std::size_t alignment);
  void deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t alignment);

  std::size_t remaining_storage(std::size_t alignment, std::size_t& wasted_due_to_alignment) const noexcept;
  void increase_next_buffer_at_least_to(std::size_t minimum_size);
  void increase_next_buffer();

  bool fills_page(std::size_t bytes) const noexcept;

  BufferPtr<void> allocate_from_current(std::size_t aligner, std::size_t bytes);

 private:
  // Up to this ratio, the buffer is used, otherwise it is just allocating a new buffer
  static constexpr float NEW_PAGE_FILL_RATIO = 0.8f;

  MemoryResource* _memory_resource;
  std::shared_ptr<SharedFrame> _current_frame;

  std::size_t _current_buffer_pos;
  std::size_t _current_buffer_size;
  std::size_t _next_buffer_size;
};

/**
 * NewDeleteMemoryResource uses classical new-delete calls, but returns a BufferPtr. Inspired by std::pmr::new_delete_resource. 
*/
class NewDeleteMemoryResource : public MemoryResource {
 public:
  BufferPtr<void> allocate(std::size_t bytes, std::size_t alignment);
  void deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t alignment);
};

}  // namespace hyrise