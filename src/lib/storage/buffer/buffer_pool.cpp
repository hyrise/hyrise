#include "buffer_pool.hpp"
#include "storage/buffer/ssd_region.hpp"

namespace hyrise {
//TODO: properly check if disabled or not
BufferPool::BufferPool(const bool enabled, const size_t pool_size, const bool enable_eviction_purge_worker,
                       std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
                       MigrationPolicy migration_policy, std::shared_ptr<SSDRegion> ssd_region,
                       std::shared_ptr<BufferPool> target_buffer_pool, const NumaMemoryNode numa_node)
    : max_bytes(pool_size),
      used_bytes(0),
      enabled(enabled),
      volatile_regions(volatile_regions),
      eviction_queue(std::make_unique<EvictionQueue>()),
      numa_node(numa_node),
      ssd_region(ssd_region),
      metrics(std::make_shared<Metrics>()),
      target_buffer_pool(target_buffer_pool),
      migration_policy(migration_policy),
      eviction_purge_worker(enable_eviction_purge_worker
                                ? std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE,
                                                                       [&](size_t) { this->purge_eviction_queue(); })
                                : nullptr) {}

void BufferPool::purge_eviction_queue() {
  auto item = EvictionItem{};
  for (auto i = size_t{0}; 0 < MAX_EVICTION_QUEUE_PURGES; ++i) {
    if (!eviction_queue->try_pop(item)) {
      return;
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // The item is in state UNLOCKED and can be marked
    if (item.can_evict(current_state_and_version) || item.can_mark(current_state_and_version)) {
      eviction_queue->push(item);
      continue;
    }
  }
}

void BufferPool::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  auto current_state_and_version = frame->state_and_version();
  DebugAssert(frame->numa_node() == numa_node, "Memory node mismatch");
  increment_counter(metrics->num_eviction_queue_adds);
  eviction_queue->push({page_id, Frame::version(current_state_and_version)});
}

void BufferPool::free_bytes(const uint64_t bytes) {
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
    if (!eviction_queue->try_pop(item)) {
      free_bytes(bytes_required);  // TODO: Check if this is correct
      return false;
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    if (frame->numa_node() != numa_node) {
      increment_counter(metrics->num_eviction_queue_items_purged);
      continue;
    }

    // If the frame is already marked, we can evict it
    if (!item.can_evict(current_state_and_version)) {
      // If the frane is UNLOCKED, we can mark it
      if (item.can_mark(current_state_and_version)) {
        if (frame->try_mark(current_state_and_version)) {
          add_to_eviction_queue(item.page_id, frame);
          continue;
        }
      }
      increment_counter(metrics->num_eviction_queue_items_purged);
      continue;
    }

    // Try locking the frame exclusively, TODO: prefer shared locking
    if (!frame->try_lock_exclusive(current_state_and_version)) {
      increment_counter(metrics->num_eviction_queue_items_purged);
      continue;
    }

    Assert(frame->numa_node() == numa_node,
           "Memory node mismatch: " + std::to_string(frame->numa_node()) + " != " + std::to_string(numa_node));

    evict(item, frame);

    increment_counter(metrics->num_evictions);

    // DebugAssert(Frame::state(frame->state_and_version()) != Frame::LOCKED, "Frame cannot be locked");

    const auto size_type = item.page_id.size_type();
    freed_bytes += bytes_for_size_type(size_type);
    current_bytes = used_bytes.load();
  }

  // TODO: Check if this is correct
  free_bytes(freed_bytes);

  return true;
}

void BufferPool::evict(EvictionItem& item, Frame* frame) {
  DebugAssert(Frame::state(frame->state_and_version()) == Frame::LOCKED, "Frame cannot be locked");
  auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
  const auto num_bytes = bytes_for_size_type(item.page_id.size_type());

  // We try to evict the current item. Based on the migration policy, we to evict the page to a lower tier.
  // If this fails, we retry and some point, we might land on SSD.
  for (auto repeat = size_t{0}; repeat < MAX_REPEAT_COUNT; ++repeat) {
    // If we have a target buffer pool and we don't want to bypass it, we move the page to the other pool
    const auto write_to_ssd =
        !target_buffer_pool || !target_buffer_pool->enabled || migration_policy.bypass_numa_during_write();

    if (write_to_ssd) {
      // Otherwise we just write the page if its dirty and free the associated pages
      if (frame->is_dirty()) {
        auto data = region->get_page(item.page_id);
        ssd_region->write_page(item.page_id, data);  // TODO: use global function
        region->protect_page(item.page_id);
        frame->reset_dirty();
      }
      region->free(item.page_id);
      frame->unlock_exclusive_and_set_evicted();

      increment_counter(metrics->total_bytes_copied_to_ssd, num_bytes);

      return;
    } else {
      // Or we just move to other numa node and unlock again
      if (!target_buffer_pool->ensure_free_pages(item.page_id.size_type())) {
        yield(repeat);
        continue;
      };
      region->mbind_to_numa_node(item.page_id, target_buffer_pool->numa_node);
      frame->unlock_exclusive();
      target_buffer_pool->add_to_eviction_queue(item.page_id, frame);
      //   TODO:increment_counter(metrics.total_bytes_copied_from_dram_to_numa, num_bytes);
      return;
    }
  }
  Fail("Could not evict page after trying for " + std::to_string(MAX_REPEAT_COUNT) + " times");
}

size_t BufferPool::memory_consumption() const {
  return sizeof(*this) + sizeof(*eviction_queue) + sizeof(EvictionQueue::value_type) * eviction_queue->unsafe_size();
}

size_t BufferPool::free_bytes_node() const {
#if HYRISE_NUMA_SUPPORT
  if (numa_node == NO_NUMA_MEMORY_NODE) {
    return 0;
  }
  long long free_bytes;
  numa_node_size(numa_node, &free_bytes);
  return free_bytes;
#else
  return 0;
#endif
};

size_t BufferPool::total_bytes_node() const {
#if HYRISE_NUMA_SUPPORT
  if (numa_node == NO_NUMA_MEMORY_NODE) {
    return 0;
  }
  return numa_node_size(numa_node, nullptr);
#else
  return 0;
#endif
};
}  // namespace hyrise