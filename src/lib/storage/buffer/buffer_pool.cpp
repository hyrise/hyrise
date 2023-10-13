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
    : max_bytes(pool_size),
      used_bytes(0),
      volatile_regions(volatile_regions),
      eviction_queue(),
      node_id(numa_node),
      persistence_manager(persistence_manager) {
  Assert(node_id != INVALID_NODE_ID, "Cannot create buffer pool with invalid numa node");
}

void BufferPool::purge_eviction_queue() {
  auto item = EvictionItem{};
  while (true) {
    if (!eviction_queue.try_dequeue(item)) {
      return;
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // The item is in state UNLOCKED and can be marked
    if (item.can_evict(current_state_and_version) || item.can_mark(current_state_and_version)) {
      eviction_queue.enqueue(item);
      continue;
    } else {
      return;
    }
  }
}

void BufferPool::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  auto current_state_and_version = frame->state_and_version();
  DebugAssert(frame->node_id() == node_id, "Memory node mismatch");
  if (num_eviction_queue_adds.fetch_add(1) % PURGE_INTERVAL == 0) {
    purge_eviction_queue();
  }
  eviction_queue.enqueue({page_id, Frame::version(current_state_and_version)});
}

void BufferPool::free_reserved_bytes(const uint64_t bytes) {
  used_bytes.fetch_sub(bytes);
}

uint64_t BufferPool::reserve_bytes(const uint64_t bytes) {
  return used_bytes.fetch_add(bytes);
}

bool BufferPool::ensure_free_pages(const PageSizeType required_size) {
  // TODO: Free at least 64 * PageSite bytes to reduce TLB shootdowns
  const auto bytes_required = bytes_for_size_type(required_size);
  auto freed_bytes = size_t{0};
  auto current_bytes = reserve_bytes(bytes_required);

  auto item = EvictionItem{};

  // Find potential victim frame if we don't have enough space left
  // TODO: Verify, that this is correct, cceh kthe numbersm, verify value type
  while ((current_bytes + bytes_required - freed_bytes) > max_bytes) {
    if (!eviction_queue.try_dequeue(item)) {
      free_reserved_bytes(bytes_required);  // TODO: Check if this is correct
      return false;
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    if (frame->node_id() != node_id) {
      increment_metric_counter(num_eviction_queue_items_purged);
      continue;
    }

    // If the frame is already marked, we can evict it
    if (!item.can_evict(current_state_and_version)) {
      // If the frame is UNLOCKED, we can mark it
      if (item.can_mark(current_state_and_version)) {
        if (frame->try_mark(current_state_and_version)) {
          add_to_eviction_queue(item.page_id, frame);
          continue;
        }
      }
      increment_metric_counter(num_eviction_queue_items_purged);
      continue;
    }

    // Try locking the frame exclusively, TODO: prefer shared locking
    if (!frame->try_lock_exclusive(current_state_and_version)) {
      increment_metric_counter(num_eviction_queue_items_purged);
      continue;
    }

    Assert(frame->node_id() == node_id,
           "Memory node mismatch: " + std::to_string(frame->node_id()) + " != " + std::to_string(node_id));

    evict(item, frame);

    increment_metric_counter(num_evictions);

    // DebugAssert(Frame::state(frame->state_and_version()) != Frame::LOCKED, "Frame cannot be locked");

    const auto size_type = item.page_id.size_type();
    freed_bytes += bytes_for_size_type(size_type);
    current_bytes = used_bytes.load();
  }

  // TODO: Check if this is correct
  free_reserved_bytes(freed_bytes);

  return true;
}

void BufferPool::evict(EvictionItem& item, Frame* frame) {
  DebugAssert(Frame::state(frame->state_and_version()) == Frame::LOCKED, "Frame cannot be locked");
  auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
  const auto num_bytes = bytes_for_size_type(item.page_id.size_type());

  // We try to evict the current item. Based on the migration policy, we to evict the page to a lower tier.
  // If this fails, we retry and some point, we might land on SSD.
  retry_with_backoff([&]() {
    // Otherwise we just write the page if its dirty and free the associated pages
    if (frame->is_dirty()) {
      auto data = region->get_page(item.page_id);
      ssd_region->write_page(item.page_id, data);  // TODO: use global function
      region->protect_page(item.page_id);
      frame->reset_dirty();
    }
    region->free(item.page_id);
    frame->unlock_exclusive_and_set_evicted();

    // TODO increment_metric_counter(total_bytes_copied_to_ssd, num_bytes);

    return true;
  });
}

size_t BufferPool::memory_consumption() const {
  return sizeof(*this) + sizeof(eviction_queue) + sizeof(EvictionItem) * eviction_queue.size_approx();
}

uint64_t BufferPool::get_num_eviction_queue_adds() const {
  return num_eviction_queue_adds.load(std::memory_order_relaxed);
}

uint64_t BufferPool::get_reserved_bytes() const {
  return used_bytes;
}

uint64_t BufferPool::get_max_bytes() const {
  return max_bytes;
}

NodeID BufferPool::get_node_id() const {
  return node_id;
}

uint64_t BufferPool::get_num_eviction_queue_items_purged() const {
  return num_eviction_queue_items_purged.load(std::memory_order_relaxed);
}

uint64_t BufferPool::get_num_evictions() const {
  return num_evictions.load(std::memory_order_relaxed);
}

std::shared_ptr<BufferPool> BufferPool::get_target_buffer_pool() const {
  return target_buffer_pool;
}

void BufferPool::resize(const uint64_t new_size) {
  max_bytes = new_size;
  Assert(ensure_free_pages(MIN_PAGE_SIZE_TYPE), "Failed to resize buffer pool");
}

}  // namespace hyrise