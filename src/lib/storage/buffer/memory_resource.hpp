#pragma once

#include <boost/container/pmr/memory_resource.hpp>
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

namespace detail {
struct LinearBufferResourceState {
  std::byte* current_buffer;
  std::size_t current_buffer_pos;
  std::size_t current_buffer_size;
};

static thread_local LinearBufferResourceState linear_buffer_resource_state = LinearBufferResourceState{nullptr, 0, 0};

}  // namespace detail

/**
 * The LinearBufferResource is a memory resource that allocates memory in pages without deallocation. Its supposed to be fast 
 * and helpful to reduce internal fragmentation for small allocations in pages when using the buffer manager. It was designed to 
 * reduce the memory usage of pmr_string from a pmr_vector. Inspired by std::pmr::monotonic_buffer_resource and adapted some code
 * from boost::container::pmr::monotonic_buffer_resource. It does not contain a release function or a destructor as we assume
 * this to be taken over by the memory ownership model of the buffer manager.
 * 
 * Properties:
 * - Initial page size is 8 KiB.
 * 
 * - Deallocation has no effect.
 * 
 *  - The memory is never released by the memory resource. The ownership of the memory is passed to the BufferPtr. When all references 
 * to a frame/page are gone, the page is released by the BufferManager. 
 * 
 * - When the current page is exhausted, a new page is allocated. The next biggest page size is used until 256 KiB is reached 
 * (thus geometrically increasing). Any new page will be 256 KiB from there on. Larger page sizes are reservered for special cases.
 * 
 * - If the bytes to be allocated fill up more than 80% of a potential page size type, a new page is allocated regardless of the current page. 
 * The existing current page is not touched and kept for the next allocation.
 * 
 * - Each thread gets its own instance of a LinearBufferResource through Tread-Local Storage with LinearBufferResource *get_monotonic_memory_resource()
*/
class LinearBufferResource : public boost::container::pmr::memory_resource, public Noncopyable {
 public:
  // First page size to be allocated for small allocations is 8 KiB
  static constexpr PageSizeType PAGE_SIZE_TYPE = PageSizeType::KiB256;

  LinearBufferResource();

  LinearBufferResource(BufferManager* buffer_manager);

  void* do_allocate(std::size_t, std::size_t) override;
  void do_deallocate(void*, std::size_t, std::size_t) override;
  bool do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept override;

  /**
    * Check how many bytes are left in the current page including the alignment.
    */
  std::size_t remaining_storage(std::size_t alignment, std::size_t& wasted_due_to_alignment) const noexcept;

  std::size_t remaining_storage(std::size_t alignment = 1u) const noexcept;

 private:
  // void increase_next_buffer_at_least_to(std::size_t minimum_size);

  // void increase_next_buffer();

  void* allocate_from_current(std::size_t aligner, std::size_t bytes);

  // Check if we can fill a new page with the given bytes at least to NEW_PAGE_FILL_RATIO (e.g. 80%)
  bool fills_page(std::size_t bytes) const;

  // Up to this ratio, the buffer is used, otherwise it is just allocating a new buffer
  static constexpr float NEW_PAGE_FILL_RATIO = 0.8f;

  BufferManager* _buffer_manager;
};

}  // namespace hyrise