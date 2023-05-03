#include "storage/buffer/memory_resource.hpp"
#include <boost/thread.hpp>
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {
MonotonicBufferResource::MonotonicBufferResource(MemoryResource* memory_resource, const PageSizeType initial_size)
    : _memory_resource(memory_resource), _current_buffer_pos(0), _current_buffer_size(0), _next_buffer_size(0) {
  increase_next_buffer_at_least_to(bytes_for_size_type(initial_size));
}

MonotonicBufferResource::MonotonicBufferResource()
    : MonotonicBufferResource(&Hyrise::get().buffer_manager, PageSizeType::KiB8) {}

std::size_t MonotonicBufferResource::remaining_storage(std::size_t alignment,
                                                       std::size_t& wasted_due_to_alignment) const noexcept {
  Assert(alignment <= PAGE_ALIGNMENT, "Alignment must not be greater than PAGE_ALIGNMENT");
  // This might not work perfectly, but at least good enough with the buffer manager as upstream since its alignment is pretty high (=512)
  const std::size_t up_alignment_minus1 = alignment - 1u;
  const std::size_t up_alignment_mask = ~up_alignment_minus1;
  const std::size_t up_addr = std::size_t(_current_buffer_pos);
  const std::size_t up_aligned_addr = (up_addr + up_alignment_minus1) & up_alignment_mask;
  wasted_due_to_alignment = std::size_t(up_aligned_addr - up_addr);
  return _current_buffer_size <= wasted_due_to_alignment ? 0u : _current_buffer_size - wasted_due_to_alignment;
}

std::size_t MonotonicBufferResource::remaining_storage(std::size_t alignment) const noexcept {
  std::size_t dummy;
  return remaining_storage(alignment, dummy);
}

BufferPtr<void> MonotonicBufferResource::allocate_from_current(std::size_t aligner, std::size_t bytes) {
  const auto buffer_pos = _current_buffer_pos + aligner;
  _current_buffer_pos = buffer_pos + bytes;
  _current_buffer_size -= aligner + bytes;
  return BufferPtr<void>(_current_frame, buffer_pos);
}

BufferPtr<void> MonotonicBufferResource::allocate(std::size_t bytes, std::size_t alignment) {
  if (alignment > alignof(std::max_align_t)) {
    Fail("Alignment must not be greater than alignof(std::max_align_t)");
  }

  if (fills_page(bytes)) {
    return _memory_resource->allocate(bytes, alignment);
  }

  std::size_t aligner = 0u;
  if (this->remaining_storage(alignment, aligner) < bytes) {
    aligner = 0u;
    this->increase_next_buffer_at_least_to(bytes);
    _current_frame = _memory_resource->allocate(_next_buffer_size, alignment).get_shared_frame();
    DebugAssert(_current_frame, "MemoryResource did not return a valid frame");
    _current_buffer_size = _next_buffer_size;
    _current_buffer_pos = 0u;
    this->increase_next_buffer();
  }

  return this->allocate_from_current(aligner, bytes);
}

void MonotonicBufferResource::increase_next_buffer_at_least_to(std::size_t minimum_size) {
  Assert(minimum_size <= bytes_for_size_type(MAX_PAGE_SIZE_TYPE), "Cannot allocate more than 256 KiB at once");
  if (minimum_size < _next_buffer_size) {
    return;
  }
  _next_buffer_size = bytes_for_size_type(find_fitting_page_size_type(minimum_size));
}

void MonotonicBufferResource::increase_next_buffer() {
  const auto next_size = bytes_for_size_type(find_fitting_page_size_type(_next_buffer_size * 2));
  if (next_size > bytes_for_size_type(MAX_PAGE_SIZE_TYPE)) {
    _next_buffer_size = bytes_for_size_type(MAX_PAGE_SIZE_TYPE);
  } else {
    _next_buffer_size = next_size;
  }
}

bool MonotonicBufferResource::fills_page(std::size_t bytes) const {
  return (double)bytes / (double)bytes_for_size_type(find_fitting_page_size_type(bytes)) >= NEW_PAGE_FILL_RATIO;
}

void MonotonicBufferResource::deallocate([[maybe_unused]] BufferPtr<void> ptr, [[maybe_unused]] std::size_t bytes,
                                         [[maybe_unused]] std::size_t alignment) {
  // Do nothing
}

BufferPtr<void> NewDeleteMemoryResource::allocate(std::size_t bytes, std::size_t alignment) {
  const auto ptr = operator new[](bytes, std::align_val_t(alignment));
  return BufferPtr<void>(ptr);
}

void NewDeleteMemoryResource::deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t alignment) {
  const auto raw_ptr = reinterpret_cast<void*>(ptr.get_offset());
  operator delete[](raw_ptr, std::align_val_t(alignment));
}

BufferManager* get_buffer_manager_memory_resource() {
  return &Hyrise::get().buffer_manager;
}

NewDeleteMemoryResource* get_new_delete_memory_resource() {
  static NewDeleteMemoryResource memory_resource;
  return &memory_resource;
}

MonotonicBufferResource* get_monotonic_buffer_resource() {
  static boost::thread_specific_ptr<MonotonicBufferResource> memory_resource;
  return memory_resource.get();
}
}  // namespace hyrise