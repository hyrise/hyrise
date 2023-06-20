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

  // Increase ref count on this buffer
  // LinearBufferResource::allocation_count(detail::linear_buffer_resource_state.current_buffer)
  //     .fetch_add(1, std::memory_order_acq_rel);

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
    // detail::linear_buffer_resource_state.current_buffer_size =
    //     bytes_for_size_type(PAGE_SIZE_TYPE) - sizeof(AllocationCountType);
    // detail::linear_buffer_resource_state.current_buffer_pos = sizeof(AllocationCountType);
    DebugAssert(detail::linear_buffer_resource_state.current_buffer_size >= bytes,
                "Buffer overflow while allocating new page");
  }

  return this->allocate_from_current(aligner, bytes);
}

bool LinearBufferResource::fills_page(std::size_t bytes) const {
  return bytes > bytes_for_size_type(PAGE_SIZE_TYPE) ||
         (double)bytes / (double)bytes_for_size_type(find_fitting_page_size_type(bytes)) >= NEW_PAGE_FILL_RATIO;
}

LinearBufferResource::AllocationCountType& LinearBufferResource::allocation_count(std::byte* buffer) noexcept {
  return *reinterpret_cast<AllocationCountType*>(buffer);
};

void LinearBufferResource::do_deallocate(void* ptr, std::size_t, std::size_t) {
  // TODO
  // auto page_id = _buffer_manager->find_page(ptr);
  // auto page_ptr = nullptr;

  // if (LinearBufferResource::allocation_count(page_ptr).fetch_sub(1, std::memory_order_release) == 1) {
  //   boost::atomic_thread_fence(boost::memory_order_acquire);
  //   _buffer_manager->do_deallocate(ptr, bytes_for_size_type(page_id.size_type()), alignof(std::max_align_t));
  // }
}

bool LinearBufferResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

}  // namespace hyrise