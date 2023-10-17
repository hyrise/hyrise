#include "buffer_pool.hpp"
#include "storage/buffer/persistence_manager.hpp"
#include "volatile_region.hpp"

namespace hyrise {
bool BufferPool::EvictionItem::can_evict(Frame::StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::MARKED && Frame::version(state_and_version) == timestamp;
}

bool BufferPool::EvictionItem::can_mark(Frame::StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::UNLOCKED && Frame::version(state_and_version) == timestamp;
}

BufferPool::BufferPool(const size_t pool_size,
                       const std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
                       const std::shared_ptr<PersistenceManager> persistence_manager, const NodeID numa_node)
    : _max_bytes(pool_size),
      _used_bytes(0),
      _volatile_regions(volatile_regions),
      _eviction_queue(),
      _node_id(numa_node),
      _persistence_manager(persistence_manager) {
  Assert(_node_id != INVALID_NODE_ID, "Cannot create buffer pool with invalid numa node");
}

void BufferPool::purge_eviction_queue() {
  auto item = EvictionItem{};
  while (true) {
    if (!_eviction_queue.try_dequeue(item)) {
      return;
    }

    auto region = _volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // The item is in state UNLOCKED and can be marked
    if (item.can_evict(current_state_and_version) || item.can_mark(current_state_and_version)) {
      _eviction_queue.enqueue(item);
      return;
    } else {
      _num_eviction_queue_items_purged.fetch_add(1, std::memory_order_relaxed);
      continue;
    }
  }
}

void BufferPool::add_to_eviction_queue(const PageID page_id) {
  const auto region = _volatile_regions[static_cast<uint64_t>(page_id.size_type())];
  const auto frame = region->get_frame(page_id);
  const auto current_state_and_version = frame->state_and_version();
  DebugAssert(frame->node_id() == _node_id, "Memory node mismatch");
  if (_num_eviction_queue_adds.fetch_add(1) % PURGE_INTERVAL == 0) {
    purge_eviction_queue();
  }
  _eviction_queue.enqueue({page_id, Frame::version(current_state_and_version)});
}

void BufferPool::free_reserved_bytes(const uint64_t bytes) {
  _used_bytes.fetch_sub(bytes);
}

uint64_t BufferPool::reserve_bytes(const uint64_t bytes) {
  return _used_bytes.fetch_add(bytes);
}

bool BufferPool::ensure_free_pages(const uint64_t bytes_required) {
  // TODO: Free at least 64 * PageSite bytes to reduce TLB shootdowns
  auto freed_bytes = size_t{0};
  auto current_bytes = reserve_bytes(bytes_required);

  auto item = EvictionItem{};

  // Find potential victim frame if we don't have enough space left
  // TODO: Verify, that this is correct, cceh kthe numbersm, verify value type
  while ((current_bytes + bytes_required - freed_bytes) > _max_bytes) {
    if (!_eviction_queue.try_dequeue(item)) {
      free_reserved_bytes(bytes_required);  // TODO: Check if this is correct
      return false;
    }

    auto region = _volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    if (frame->node_id() != _node_id) {
      _num_eviction_queue_items_purged.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    // If the frame is already marked, we can evict it
    if (!item.can_evict(current_state_and_version)) {
      // If the frame is UNLOCKED, we can mark it
      if (item.can_mark(current_state_and_version)) {
        if (frame->try_mark(current_state_and_version)) {
          add_to_eviction_queue(item.page_id);
          continue;
        }
      }
      _num_eviction_queue_items_purged.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    // Try locking the frame exclusively, TODO: prefer shared locking
    if (!frame->try_lock_exclusive(current_state_and_version)) {
      _num_eviction_queue_items_purged.fetch_add(1, std::memory_order_relaxed);
      continue;
    }

    evict(item, frame);

    _num_evictions.fetch_add(1, std::memory_order_relaxed);

    const auto size_type = item.page_id.size_type();
    freed_bytes += bytes_for_size_type(size_type);
    current_bytes = _used_bytes.load();
  }

  free_reserved_bytes(freed_bytes);

  return true;
}

void BufferPool::evict(EvictionItem& item, Frame* frame) {
  DebugAssert(Frame::state(frame->state_and_version()) == Frame::LOCKED, "Frame cannot be locked");
  auto region = _volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
  const auto num_bytes = bytes_for_size_type(item.page_id.size_type());

  // Otherwise we just write the page if its dirty and free the associated pages
  if (frame->is_dirty()) {
    auto data = region->get_page(item.page_id);
    _persistence_manager->write_page(item.page_id, data);  // TODO: use global function
    frame->reset_dirty();
  }
  region->_protect_page(item.page_id);
  region->free(item.page_id);
  frame->unlock_exclusive_and_set_evicted();

  _total_bytes_evicted_to_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
}

size_t BufferPool::memory_consumption() const {
  return sizeof(*this) + sizeof(EvictionItem) * _eviction_queue.size_approx();
}

uint64_t BufferPool::num_eviction_queue_adds() const {
  return _num_eviction_queue_adds.load(std::memory_order_relaxed);
}

uint64_t BufferPool::reserved_bytes() const {
  return _used_bytes.load(std::memory_order_relaxed);
}

uint64_t BufferPool::max_bytes() const {
  return _max_bytes;
}

uint64_t BufferPool::total_bytes_evicted_to_ssd() const {
  return _total_bytes_evicted_to_ssd.load(std::memory_order_relaxed);
}

NodeID BufferPool::node_id() const {
  return _node_id;
}

uint64_t BufferPool::num_eviction_queue_items_purged() const {
  return _num_eviction_queue_items_purged.load(std::memory_order_relaxed);
}

uint64_t BufferPool::num_evictions() const {
  return _num_evictions.load(std::memory_order_relaxed);
}

void BufferPool::resize(const uint64_t new_size) {
  _max_bytes = new_size;
  if (!ensure_free_pages(0)) {
    Fail("Failed to resize buffer pool");
  }
}

}  // namespace hyrise
