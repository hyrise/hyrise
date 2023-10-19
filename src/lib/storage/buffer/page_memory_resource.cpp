#include "allocator.hpp"
#include "page_id.hpp"

namespace hyrise {

// Find the smallest page size type that can fit a given number of bytes
constexpr PageSizeType find_fitting_page_size_type(const std::size_t bytes) {
  for (auto page_size_type : magic_enum::enum_values<PageSizeType>()) {
    if (bytes <= bytes_for_size_type(page_size_type)) {
      return page_size_type;
    }
  }
  Fail("Cannot fit value of " + std::to_string(bytes) + " bytes to a PageSizeType");
}

PageMemoryResource::PageMemoryResource(BufferManager* buffer_manager)
    : _buffer_manager(buffer_manager), _mutexes(), _num_pages() {}

PageID PageMemoryResource::new_page_id(const PageSizeType size_type) {
  auto lock_guard = std::scoped_lock{_mutexes[static_cast<uint64_t>(size_type)]};

  if (_free_page_ids[static_cast<uint64_t>(size_type)].empty()) {
    const auto page_idx = _num_pages[static_cast<uint64_t>(size_type)]++;
    return PageID{size_type, page_idx, true};
  } else {
    const auto page_id = _free_page_ids[static_cast<uint64_t>(size_type)].top();
    _free_page_ids[static_cast<uint64_t>(size_type)].pop();
    return page_id;
  }
}

void PageMemoryResource::free_page_id(const PageID page_id) {
  auto lock_guard = std::scoped_lock{_mutexes[static_cast<uint64_t>(page_id.size_type())]};
  _free_page_ids[static_cast<uint64_t>(page_id.size_type())].push(page_id);
}

void* PageMemoryResource::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto size_type = find_fitting_page_size_type(bytes);
  const auto page_id = new_page_id(size_type);
  const auto region = _buffer_manager->_volatile_regions[static_cast<uint64_t>(size_type)];
  const auto frame = region->get_frame(page_id);
  const auto ptr = region->get_page(page_id);

  const auto state_and_version = frame->state_and_version();
  if (!frame->try_lock_exclusive(state_and_version)) {
    Fail("Could not lock page for exclusive access. This should not happen during an allocation.");
  }

  region->_unprotect_page(page_id);
  retry_with_backoff(
      [&, page_id = page_id]() { return _buffer_manager->_buffer_pool.ensure_free_pages(page_id.num_bytes()); });
  region->mbind_to_numa_node(page_id, _buffer_manager->_buffer_pool.node_id());
  frame->set_dirty(true);
  frame->unlock_exclusive();
  _buffer_manager->_buffer_pool.add_to_eviction_queue(page_id);

  _allocation_count.fetch_add(1, std::memory_order_relaxed);
  _allocated_bytes.fetch_add(bytes_for_size_type(size_type), std::memory_order_relaxed);

  return ptr;
}

void PageMemoryResource::do_deallocate(void* ptr, std::size_t bytes, std::size_t alignment) {
  const auto page_id = _buffer_manager->find_page(ptr);

  const auto region = _buffer_manager->_volatile_regions[static_cast<uint64_t>(page_id.size_type())];
  auto frame = region->get_frame(page_id);
  if (!frame->try_lock_exclusive(frame->state_and_version())) {
    Fail("Could not lock page for exclusive access. This should not happen during a deallocation.");
  }
  frame->reset_dirty();
  frame->unlock_exclusive();
  _buffer_manager->_buffer_pool.add_to_eviction_queue(page_id);

  free_page_id(page_id);

  _deallocation_count.fetch_add(1, std::memory_order_relaxed);
  _allocated_bytes.fetch_sub(bytes_for_size_type(page_id.size_type()), std::memory_order_relaxed);
}

bool PageMemoryResource::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

uint64_t PageMemoryResource::allocation_count() const {
  return _allocation_count.load(std::memory_order_relaxed);
}

uint64_t PageMemoryResource::deallocation_count() const {
  return _deallocation_count.load(std::memory_order_relaxed);
}

uint64_t PageMemoryResource::allocated_bytes() const {
  return _allocated_bytes.load(std::memory_order_relaxed);
}

}  // namespace hyrise