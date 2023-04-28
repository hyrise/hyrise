#include "storage/buffer/memory_resource.hpp"
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {
MonotonicBufferResource::MonotonicBufferResource(MemoryResource* memory_resource, const std::size_t initial_size)
    : _memory_resource(memory_resource), _current_buffer_pos(0), _current_buffer_size(0), _next_buffer_size(0) {
  Assert(initial_size > 0, "Initial size must be greater than 0");
  increase_next_buffer_at_least_to(initial_size);
}

MonotonicBufferResource::MonotonicBufferResource()
    : MonotonicBufferResource(&Hyrise::get().buffer_manager, bytes_for_size_type(PageSizeType::KiB8)) {}

std::size_t MonotonicBufferResource::remaining_storage(std::size_t alignment,
                                                       std::size_t& wasted_due_to_alignment) const noexcept {
  // TODO: This might not work perfectly, but at least good enough with the buffer manager as upstream
  const std::size_t up_alignment_minus1 = alignment - 1u;
  const std::size_t up_alignment_mask = ~up_alignment_minus1;
  const std::size_t up_addr = std::size_t(_current_buffer_pos);
  const std::size_t up_aligned_addr = (up_addr + up_alignment_minus1) & up_alignment_mask;
  wasted_due_to_alignment = std::size_t(up_aligned_addr - up_addr);
  return _current_buffer_size <= wasted_due_to_alignment ? 0u : _current_buffer_size - wasted_due_to_alignment;
}

BufferPtr<void> MonotonicBufferResource::allocate_from_current(std::size_t aligner, std::size_t bytes) {
  auto buffer_pos = _current_buffer_pos + aligner;
  _current_buffer_pos = buffer_pos + bytes;
  _current_buffer_size -= aligner + bytes;
  return BufferPtr<void>(_current_frame, buffer_pos);
}

BufferPtr<void> MonotonicBufferResource::allocate(std::size_t bytes, std::size_t alignment) {
  if (alignment > alignof(std::max_align_t)) {
    Fail("Alignment must not be greater than alignof(std::max_align_t)");
  }

  std::size_t aligner = 0u;
  if (this->remaining_storage(alignment, aligner) < bytes) {
    aligner = 0u;
    this->increase_next_buffer_at_least_to(bytes);
    _current_frame = _memory_resource->allocate(_next_buffer_size, alignment).get_shared_frame();
    Assert(_current_frame, "MemoryResource did not return a valid frame");
    _current_buffer_size = _next_buffer_size;
    this->increase_next_buffer();
  }

  return this->allocate_from_current(aligner, bytes);
}

void MonotonicBufferResource::increase_next_buffer_at_least_to(std::size_t minimum_size) {
  _next_buffer_size = bytes_for_size_type(find_fitting_page_size_type(minimum_size));
}

void MonotonicBufferResource::increase_next_buffer() {
  _next_buffer_size = (std::size_t(-1) / 2 < _next_buffer_size) ? std::size_t(-1) : _next_buffer_size * 2;
}

void MonotonicBufferResource::deallocate([[maybe_unused]] BufferPtr<void> ptr, [[maybe_unused]] std::size_t bytes,
                                         [[maybe_unused]] std::size_t alignment) {
  // Do nothing
}

BufferPtr<void> NewDeleteMemoryResource::allocate(std::size_t bytes, std::size_t alignment) {
  return BufferPtr<void>(new std::byte[bytes]);
}

void NewDeleteMemoryResource::deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t alignment) {
  delete[] (reinterpret_cast<std::byte*>(ptr.get_offset()));
}

}  // namespace hyrise