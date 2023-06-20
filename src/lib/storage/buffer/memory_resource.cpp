#include "storage/buffer/memory_resource.hpp"
#include <boost/thread.hpp>
#include "hyrise.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

static thread_local LinearBufferResource linear_buffer_resource = LinearBufferResource(&BufferManager::get());

LinearBufferResource::LinearBufferResource(BufferManager* buffer_manager) : _buffer_manager(buffer_manager) {}

LinearBufferResource::LinearBufferResource() : LinearBufferResource(nullptr) {}

std::size_t LinearBufferResource::remaining_storage(std::size_t alignment,
                                                    std::size_t& wasted_due_to_alignment) const noexcept {
  Assert(alignment <= PAGE_ALIGNMENT, "Alignment must not be greater than PAGE_ALIGNMENT");
  // This might not work perfectly, but at least good enough with the buffer manager as upstream since its alignment is pretty high (=512)
  const std::size_t up_alignment_minus1 = alignment - 1u;
  const std::size_t up_alignment_mask = ~up_alignment_minus1;
  const std::size_t up_addr = std::size_t(detail::linear_buffer_resource_state.current_buffer_pos);
  const std::size_t up_aligned_addr = (up_addr + up_alignment_minus1) & up_alignment_mask;
  wasted_due_to_alignment = std::size_t(up_aligned_addr - up_addr);
  return detail::linear_buffer_resource_state.current_buffer_size <= wasted_due_to_alignment
             ? 0u
             : detail::linear_buffer_resource_state.current_buffer_size - wasted_due_to_alignment;
}

std::size_t LinearBufferResource::remaining_storage(std::size_t alignment) const noexcept {
  std::size_t dummy;
  return remaining_storage(alignment, dummy);
}

void* LinearBufferResource::allocate_from_current(std::size_t aligner, std::size_t bytes) {
  const auto buffer_pos = detail::linear_buffer_resource_state.current_buffer_pos + aligner;
  detail::linear_buffer_resource_state.current_buffer_pos = buffer_pos + bytes;
  DebugAssert(detail::linear_buffer_resource_state.current_buffer_pos <= bytes_for_size_type(PAGE_SIZE_TYPE),
              "Buffer overflow");
  detail::linear_buffer_resource_state.current_buffer_size -= aligner + bytes;

  return detail::linear_buffer_resource_state.current_buffer + buffer_pos;
}

void* LinearBufferResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  if (alignment > alignof(std::max_align_t)) {
    Fail("Alignment must not be greater than alignof(std::max_align_t)");
  }

  if (fills_page(bytes)) {
    return _buffer_manager->allocate(bytes, alignment);
  }

  std::size_t aligner = 0u;
  if (this->remaining_storage(alignment, aligner) < bytes) {
    aligner = 0u;
    detail::linear_buffer_resource_state.current_buffer = static_cast<std::byte*>(
        _buffer_manager->allocate(bytes_for_size_type(PAGE_SIZE_TYPE), alignof(std::max_align_t)));
    detail::linear_buffer_resource_state.current_buffer_size = bytes_for_size_type(PAGE_SIZE_TYPE);
    detail::linear_buffer_resource_state.current_buffer_pos = 0u;
    DebugAssert(detail::linear_buffer_resource_state.current_buffer_size >= bytes,
                "Buffer overflow while allocating new page");
  }

  return this->allocate_from_current(aligner, bytes);
}

bool LinearBufferResource::fills_page(std::size_t bytes) const {
  return bytes > bytes_for_size_type(PAGE_SIZE_TYPE) ||
         (double)bytes / (double)bytes_for_size_type(find_fitting_page_size_type(bytes)) >= NEW_PAGE_FILL_RATIO;
}

void LinearBufferResource::do_deallocate(void*, std::size_t, std::size_t) {
  // TODO
}

bool LinearBufferResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

}  // namespace hyrise