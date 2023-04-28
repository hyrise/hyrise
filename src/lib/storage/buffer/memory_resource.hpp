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
 * Inspired by std::pmr::monotonic_buffer_resource and adapted some code from boost::container::pmr::monotonic_buffer_resource
 * TODO: Verify usage for shared_frame (maybe pin)
 * TODO: Switch to different page sizes if fill size >80% of possible page size
*/
class MonotonicBufferResource : public MemoryResource {
 public:
  MonotonicBufferResource();
  MonotonicBufferResource(MemoryResource* memory_resource,
                          const std::size_t initial_size = bytes_for_size_type(PageSizeType::KiB8));

  BufferPtr<void> allocate(std::size_t bytes, std::size_t alignment);
  void deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t alignment);

  std::size_t remaining_storage(std::size_t alignment, std::size_t& wasted_due_to_alignment) const noexcept;
  void increase_next_buffer_at_least_to(
      std::size_t
          minimum_size);  // TODO: Incoproate page sizes a bit better here, we jump weirdly and the allocation is also weird with the page sizes
  void increase_next_buffer();

  BufferPtr<void> allocate_from_current(std::size_t aligner, std::size_t bytes);

 private:
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