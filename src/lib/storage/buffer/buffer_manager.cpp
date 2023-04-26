#include "buffer_manager.hpp"
#include <algorithm>
#include <chrono>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_ptr.hpp"
#include "storage/buffer/env.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

//----------------------------------------------------
// Helper Functions

BufferManager::Config BufferManager::Config::from_env() {
  auto config = BufferManager::Config{};
  config.dram_buffer_pool_size = get_dram_capacity_from_env();
  config.ssd_path = get_ssd_region_file_from_env();
  config.migration_policy = get_migration_policy_from_env();
  config.numa_memory_node = get_numa_node_from_env();
  return config;
}

//----------------------------------------------------
// Buffer Pools
//----------------------------------------------------

BufferManager::BufferPools::BufferPools(const PageType page_type, const size_t pool_size,
                                        const std::function<void(std::shared_ptr<Frame>&)> evict_frame,
                                        const bool enable_eviction_worker,
                                        const std::shared_ptr<BufferManager::Metrics> metrics)
    : _used_bytes(0),
      enabled(_page_type != PageType::Invalid),
      _evict_frame(evict_frame),
      _total_bytes(pool_size),
      _page_type(page_type),

      _metrics(metrics) {
  if (enabled) {
    _eviction_queue = std::make_shared<EvictionQueue>();
    _buffer_pools = create_volatile_regions_for_size_types(page_type, pool_size);
    if (enable_eviction_worker) {
      _eviction_worker = std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE,
                                                              [&](size_t) { this->purge_eviction_queue(); });
    }
  }
}

BufferManager::BufferPools& BufferManager::BufferPools::operator=(BufferManager::BufferPools&& other) {
  _used_bytes = other._used_bytes.load();
  _total_bytes = other._total_bytes;
  _page_type = other._page_type;
  _page_type = other._page_type;
  // _evict_frame = other._evict_frame;
  enabled = other.enabled;
  _metrics = other._metrics;
  // Assert(_total_bytes == other._total_bytes, "Cannot move buffer pools with different sizes");
  // Assert(_page_type == other._page_type, "Cannot move buffer pools with different page types");
  _buffer_pools = std::move(other._buffer_pools);
  _eviction_worker = std::move(other._eviction_worker);
  _eviction_queue = std::move(other._eviction_queue);

  return *this;
}

// TODO: Track purges, verifiy proper eviction
void BufferManager::BufferPools::allocate_frame(std::shared_ptr<Frame> frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");
  const auto required_size_type = frame->size_type;

  // This potentially overshoots the _used_bytes to reserves space for the new frame.
  // If this happens concurrently, we just continue evicting in each thread until we have enough space.
  const auto bytes_required = bytes_for_size_type(required_size_type);
  auto freed_bytes = size_t{0};
  auto current_bytes = _used_bytes.fetch_add(bytes_required);

  // Find potential victim frame if we don't have enough space left
  auto item = EvictionItem{};
  while ((current_bytes + bytes_required - freed_bytes) > _total_bytes) {
    if (!_eviction_queue->try_pop(item)) {
      Fail(
          "Cannot pop item from queue. All frames seems to be pinned. Please increase the memory size of this buffer "
          "pool.");
    }

    // Check if the frame is still valid
    auto current_frame = item.frame.lock();
    if (!current_frame || !current_frame->can_evict()) {
      continue;
    }

    // Lock the frame to prevent it from being evicted while we are evicting it
    // Check if the frame can be evicted based on the timestamp, the state and the pin count
    std::lock_guard<std::mutex> lock(current_frame->latch);
    DebugAssert(current_frame->page_type == _page_type, "Frame has wrong page type");
    if (!item.can_evict(current_frame)) {
      continue;
    }

    // Check if the frame is second chance evictable. If not, just requeue it and remove the reference bit.
    if (!current_frame->try_second_chance_evictable()) {
      _eviction_queue->push(item);
      continue;
    }

    // Call the callback to evict the frame. This is handled by the BufferManager instead of the BufferPool,
    // because we need access other members for eviction.
    _evict_frame(current_frame);

    const auto current_frame_size_type = current_frame->size_type;
    freed_bytes += bytes_for_size_type(current_frame_size_type);
    current_bytes = _used_bytes.load();

    if (current_frame_size_type == required_size_type) {
      // If the evicted frame is of the same page type, we should just use it directly.
      _buffer_pools[static_cast<size_t>(required_size_type)]->move(current_frame, frame);
      break;
    } else {
      // Otherwise, release physical pages the current frame and continue evicting
      _buffer_pools[static_cast<size_t>(current_frame_size_type)]->free(current_frame);
      if (_page_type == PageType::Dram) {
        _metrics->num_madvice_free_calls_dram++;
      } else if (_page_type == PageType::Numa) {
        _metrics->num_madvice_free_calls_numa++;
      } else {
        Fail("Invalid page type");
      }
      current_bytes -= freed_bytes;
    }
  }

  // We need to subtract the freed bytes from the used bytes again,
  // since we overshot the _used_bytes in the beginning.
  _used_bytes -= freed_bytes;

  // If we did not find a page of the correct size, we allocate a new one.
  // We freed enough space in the previous steps.
  if (!frame->data) {
    _buffer_pools[static_cast<size_t>(required_size_type)]->allocate(frame);
  }
}

void BufferManager::BufferPools::deallocate_frame(std::shared_ptr<Frame> frame) {
  // TODO: add_to_eviction_queue(frame); ?, might reduce madvie calls
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");
  if (frame->is_resident()) {
    std::lock_guard<std::mutex> lock(frame->latch);
    const auto size_type = frame->size_type;
    _buffer_pools[static_cast<size_t>(size_type)]->free(frame);
    _used_bytes -= bytes_for_size_type(size_type);
    if (_page_type == PageType::Dram) {
      _metrics->num_madvice_free_calls_dram++;
    } else if (_page_type == PageType::Numa) {
      _metrics->num_madvice_free_calls_numa++;
    } else {
      Fail("Invalid page type");
    }
  }
  frame->clear();
}

void BufferManager::BufferPools::purge_eviction_queue() {
  auto item = EvictionItem{};

  // Copy pointer to eviction queue to ensure access safety when task is running, while the buffer manager
  // was already destroyed
  auto eviction_queue = _eviction_queue;
  if (!eviction_queue) {
    return;
  }

  for (auto i = size_t{0}; i < MAX_EVICTION_QUEUE_PURGES; i++) {
    // Check if the queue is empty
    if (!eviction_queue->try_pop(item)) {
      break;
    }

    // If frame is gone or the timestamp is outdated, we can purge the item
    auto frame = item.frame.lock();
    if (!frame || !item.can_evict(frame)) {
      if (_page_type == PageType::Dram) {
        _metrics->num_dram_eviction_queue_items_purged++;
      } else if (_page_type == PageType::Numa) {
        _metrics->num_numa_eviction_queue_items_purged++;
      } else {
        Fail("Invalid page type");
      }
      continue;
    } else {
      // If not, just requeue the item
      eviction_queue->push(item);
    }
  }
}

void BufferManager::BufferPools::add_to_eviction_queue(std::shared_ptr<Frame> frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  // Insert the frame into the eviction queue with an updated timestamp
  const auto timestamp_before_eviction = frame->eviction_timestamp++;
  _eviction_queue->push({frame, timestamp_before_eviction + 1});

  _metrics->num_dram_eviction_queue_adds++;
}

// Write clear method for BufferPools
void BufferManager::BufferPools::clear() {
  _used_bytes = 0;
  _eviction_queue->clear();
  for (auto& pool : _buffer_pools) {
    pool->clear();
  }
}

//----------------------------------------------------
// Buffer Manager
//----------------------------------------------------

BufferManager::BufferManager(const Config config)
    : _num_pages(0),
      _config(config),
      _migration_policy(config.migration_policy),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path)),
      _metrics(std::make_shared<Metrics>()),
      _dram_buffer_pools(page_type_for_dram_buffer_pools(config.mode), config.dram_buffer_pool_size,
                         std::bind(&BufferManager::evict_frame, this, std::placeholders::_1),
                         config.enable_eviction_worker, _metrics),
      _numa_buffer_pools(page_type_for_numa_buffer_pools(config.mode), config.numa_buffer_pool_size,
                         std::bind(&BufferManager::evict_frame, this, std::placeholders::_1),
                         config.enable_eviction_worker, _metrics) {
  Assert(_dram_buffer_pools.enabled || _numa_buffer_pools.enabled, "No Buffer Pool is enabled");
}

BufferManager::Config BufferManager::get_config() const {
  return _config;
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

void BufferManager::make_resident(std::shared_ptr<Frame> frame) {
  if (frame->is_resident()) {
    if (frame->page_type == PageType::Dram) {
      _metrics->total_hits_dram++;
    } else if (frame->page_type == PageType::Numa) {
      _metrics->total_hits_numa++;
    } else {
      Fail("Invalid page type");
    }
    return;
  }
  std::lock_guard<std::mutex> frame_lock(frame->latch);
  DebugAssert(!frame->is_resident(), "Frame is already resident");

  // Read from SSD if its a NUMA frame or if only DRAM is enabled
  if (frame->page_type == PageType::Numa) {
    _metrics->total_misses_numa++;
    _numa_buffer_pools.allocate_frame(frame);
    _numa_buffer_pools.add_to_eviction_queue(frame);
    read_page_from_ssd(frame);
    frame->set_resident();
    return;
  }

  if (frame->page_type == PageType::Dram && !_numa_buffer_pools.enabled) {
    _metrics->total_misses_dram++;
    _dram_buffer_pools.allocate_frame(frame);
    _dram_buffer_pools.add_to_eviction_queue(frame);
    read_page_from_ssd(frame);
    frame->set_resident();
    return;
  }

  auto shared_frame = frame->shared_frame.lock();
  if (!shared_frame) {
    Fail("Frame has no shared frame");
  }
  _metrics->total_misses_dram++;

  // If the numa frame is not there or if the data is not resident in NUMA, we read it from the SSD
  auto numa_frame = shared_frame->numa_frame;
  if (!numa_frame || !numa_frame->is_resident()) {
    _metrics->total_misses_numa++;
    _dram_buffer_pools.allocate_frame(frame);
    _dram_buffer_pools.add_to_eviction_queue(frame);
    read_page_from_ssd(frame);
    frame->set_resident();
    return;
  }
  _metrics->total_hits_numa++;

  // TODO: Do we need to pin?, use a spinlock? here until resident
  DebugAssert(!numa_frame->is_pinned(), "NUMA frame is pinned");
  std::lock_guard<std::mutex> numa_frame_lock(numa_frame->latch);

  const auto num_bytes = bytes_for_size_type(frame->size_type);
  _dram_buffer_pools.allocate_frame(frame);
  _dram_buffer_pools.add_to_eviction_queue(frame);
  std::memcpy(frame->data, numa_frame->data, num_bytes);
  _metrics->total_bytes_copied_from_numa_to_dram += num_bytes;

  frame->set_resident();
}

// rename to evictBlocks(...)
std::shared_ptr<Frame>& BufferManager::load_frame(const std::shared_ptr<SharedFrame>& shared_frame,
                                                  const AccessIntent access_intent) {
  // Return the same block if its pinned
  if (_dram_buffer_pools.enabled && shared_frame->dram_frame && shared_frame->dram_frame->is_resident()) {
    // 1. Found the frame in DRAM
    make_resident(shared_frame->dram_frame);
    return shared_frame->dram_frame;
  } else if (_numa_buffer_pools.enabled && shared_frame->numa_frame && shared_frame->numa_frame->is_resident()) {
    // 2. Found the frame on NUMA and we want to migrate it to DRAM TODO: read or write intent?!
    const auto bypass_dram = !_dram_buffer_pools.enabled || _migration_policy.bypass_dram_during_read();
    if (bypass_dram) {
      // 3. We want to bypass dram
      make_resident(shared_frame->numa_frame);
      return shared_frame->numa_frame;
    }

    // 4. Lets not bypass dram and init a new DRAM frame
    if (!shared_frame->dram_frame) {
      auto size_type = shared_frame->numa_frame->size_type;
      auto page_id = shared_frame->numa_frame->page_id;
      const auto frame = std::make_shared<Frame>(page_id, size_type, PageType::Dram);
      _dram_buffer_pools.allocate_frame(frame);
      _dram_buffer_pools.add_to_eviction_queue(frame);
      SharedFrame::link(shared_frame, frame);
    }
    make_resident(shared_frame->dram_frame);

    return shared_frame->dram_frame;
  } else if (shared_frame->dram_frame || shared_frame->numa_frame) {
    // 5. The page is neither resident DRAM nor on NUMA, we need to load it from SSD
    // Use DRAM is enabled and if we want to bypass, or if NUMA is disabled
    const auto use_dram =
        !_numa_buffer_pools.enabled || (_dram_buffer_pools.enabled && _migration_policy.bypass_numa_during_read());
    // TODO: Require lock of tier
    // TODO: Only read from SSD if data exists there
    if (use_dram) {
      if (!shared_frame->dram_frame) {
        auto size_type = shared_frame->numa_frame->size_type;
        auto page_id = shared_frame->numa_frame->page_id;
        const auto frame = std::make_shared<Frame>(page_id, size_type, PageType::Dram);
        _dram_buffer_pools.allocate_frame(frame);
        _dram_buffer_pools.add_to_eviction_queue(frame);
        SharedFrame::link(shared_frame, frame);
      }
      make_resident(shared_frame->dram_frame);
      return shared_frame->dram_frame;
    } else {
      if (!shared_frame->numa_frame) {
        auto size_type = shared_frame->dram_frame->size_type;
        auto page_id = shared_frame->dram_frame->page_id;
        const auto frame = std::make_shared<Frame>(page_id, size_type, PageType::Numa);
        _numa_buffer_pools.allocate_frame(frame);
        _numa_buffer_pools.add_to_eviction_queue(frame);
        SharedFrame::link(shared_frame, frame);
      }
      make_resident(shared_frame->numa_frame);
      return shared_frame->numa_frame;
    }
  } else {
    Fail("Shared frame without any frames");
  }
}

// TODO: Pass this to constructor of BufferPool
void BufferManager::evict_frame(std::shared_ptr<Frame>& frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->is_resident(), "Frame is not resident");
  DebugAssert(!frame->is_pinned(), "Frame is still pinned");

  if (frame->dirty) {
    // Evict to SSD if it's a NUMA frame or if it's a DRAM frame and we can bypass NUMA
    auto shared_frame = frame->shared_frame.lock();
    auto evict_to_ssd = frame->page_type == PageType::Numa || !shared_frame ||
                        (frame->page_type == PageType::Dram && !_numa_buffer_pools.enabled) ||
                        (frame->page_type == PageType::Dram && _numa_buffer_pools.enabled &&
                         _migration_policy.bypass_numa_during_write());

    if (evict_to_ssd) {
      // 1. Evict to SSD
      write_page_to_ssd(frame);
    } else {
      // TODO: Evict to NUMA if not dirty?, check if numa frame exists
      // 2. Evict to NUMA
      auto allocated_numa_frame = std::make_shared<Frame>(frame->page_id, frame->size_type, PageType::Numa);
      _numa_buffer_pools.allocate_frame(allocated_numa_frame);
      const auto num_bytes = bytes_for_size_type(frame->size_type);
      std::memcpy(allocated_numa_frame->data, frame->data, num_bytes);
      _metrics->total_bytes_copied_from_dram_to_numa += num_bytes;
      _numa_buffer_pools.add_to_eviction_queue(allocated_numa_frame);

      SharedFrame::link(shared_frame, allocated_numa_frame);
    }
    frame->dirty = false;
  }

  frame->set_evicted();
}

void BufferManager::read_page_from_ssd(std::shared_ptr<Frame> frame) {
  _ssd_region->read_page(frame);
  const auto num_bytes = bytes_for_size_type(frame->size_type);
  _metrics->total_bytes_copied_from_ssd += num_bytes;

  if (frame->page_type == PageType::Dram) {
    _metrics->total_bytes_copied_from_ssd_to_dram += num_bytes;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->total_bytes_copied_from_ssd_to_numa += num_bytes;
  } else {
    Fail("Invalid page type");
  }
}

void BufferManager::write_page_to_ssd(std::shared_ptr<Frame> frame) {
  _ssd_region->write_page(frame);
  const auto num_bytes = bytes_for_size_type(frame->size_type);
  _metrics->total_bytes_copied_to_ssd += num_bytes;
  if (frame->page_type == PageType::Dram) {
    _metrics->total_bytes_copied_from_dram_to_ssd += num_bytes;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->total_bytes_copied_from_numa_to_ssd += num_bytes;
  } else {
    Fail("Invalid page type");
  }
}

std::shared_ptr<SharedFrame> BufferManager::new_frame(const PageSizeType size_type) {
  const auto page_id = PageID{_num_pages++};

  // TODO: We could just defer this to the first time the page is evicted
  _ssd_region->allocate(page_id, size_type);

  const auto use_dram =
      !_numa_buffer_pools.enabled || (_dram_buffer_pools.enabled && !_migration_policy.bypass_dram_during_write());

  if (use_dram) {
    auto allocated_dram_frame = std::make_shared<Frame>(page_id, size_type, PageType::Dram);
    auto shared_frame = std::make_shared<SharedFrame>(allocated_dram_frame);
    allocated_dram_frame->shared_frame = shared_frame;
    _dram_buffer_pools.allocate_frame(allocated_dram_frame);
    _dram_buffer_pools.add_to_eviction_queue(allocated_dram_frame);  // TODO: remove, rely on pin
    return shared_frame;
  } else {
    auto allocated_numa_frame = std::make_shared<Frame>(page_id, size_type, PageType::Numa);
    auto shared_frame = std::make_shared<SharedFrame>(allocated_numa_frame);
    allocated_numa_frame->shared_frame = shared_frame;
    _numa_buffer_pools.allocate_frame(allocated_numa_frame);
    _numa_buffer_pools.add_to_eviction_queue(allocated_numa_frame);  // TODO: remove, rely on pin
    return shared_frame;
  }
}

void BufferManager::unpin(std::shared_ptr<Frame> frame, const bool dirty) {
  if (frame->page_type == PageType::Dram) {
    _metrics->current_pins_dram--;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->current_pins_numa--;
  } else {
    Fail("Invalid page type");
  }

  frame->set_referenced();
  frame->try_set_dirty(dirty);

  // Add to eviction queue if the frame is not pinned anymore
  const auto old_pin_count = frame->pin_count--;
  if (old_pin_count == 1) {
    if (frame->page_type == PageType::Dram) {
      _dram_buffer_pools.add_to_eviction_queue(frame);
    } else if (frame->page_type == PageType::Numa) {
      _numa_buffer_pools.add_to_eviction_queue(frame);
    } else {
      Fail("Invalid page type");
    }
  }
}

void BufferManager::pin(std::shared_ptr<Frame> frame) {
  frame->pin_count++;

  if (frame->page_type == PageType::Dram) {
    _metrics->current_pins_dram++;
    _metrics->total_pins_dram++;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->current_pins_numa++;
    _metrics->total_pins_numa++;
  } else {
    Fail("Invalid page type");
  }

  make_resident(frame);

  DebugAssert(frame->is_resident(), "Frame should be resident");
}

std::pair<std::shared_ptr<Frame>, std::ptrdiff_t> BufferManager::unswizzle(const void* ptr) {
  if (_dram_buffer_pools.enabled) {
    for (auto& buffer_pool : _dram_buffer_pools._buffer_pools) {
      if (const auto frame = buffer_pool->unswizzle(ptr)) {
        return std::make_pair(frame, reinterpret_cast<const std::byte*>(ptr) - frame->data);
      }
    }
  }
  if (_numa_buffer_pools.enabled) {
    for (auto& buffer_pool : _numa_buffer_pools._buffer_pools) {
      if (const auto frame = buffer_pool->unswizzle(ptr)) {
        return std::make_pair(frame, reinterpret_cast<const std::byte*>(ptr) - frame->data);
      }
    }
  }

  return std::make_pair(nullptr, 0);
};

BufferPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  const auto page_size_type = find_fitting_page_size_type(bytes);

  const auto shared_frame = new_frame(page_size_type);

  if (shared_frame->dram_frame) {
    _metrics->current_bytes_used_dram += bytes;
    _metrics->total_unused_bytes_dram += bytes_for_size_type(page_size_type) - bytes;
    _metrics->total_allocated_bytes_dram += bytes;
  } else if (shared_frame->numa_frame) {
    _metrics->current_bytes_used_numa += bytes;
    _metrics->total_unused_bytes_numa += bytes_for_size_type(page_size_type) - bytes;
    _metrics->total_allocated_bytes_numa += bytes;
  } else {
    Fail("Invalid page type");
  }

  _metrics->num_allocs++;
  return BufferPtr<void>(shared_frame, 0);
}

void BufferManager::deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t align) {
  _metrics->num_deallocs++;
  auto shared_frame = ptr._shared_frame;

  if (!shared_frame) {
    // TODO: This case happens with the MVCC data structures when running the filebased benchmark
    return;
  }

  // Deallocate the frames from their buffer pools and free some memory
  if (shared_frame->dram_frame) {
    _metrics->current_pins_dram -= shared_frame->dram_frame->pin_count.load();  // TODO: Remove this later
    _metrics->current_bytes_used_dram -= bytes;
    _dram_buffer_pools.deallocate_frame(shared_frame->dram_frame);
  }

  if (shared_frame->numa_frame) {
    _metrics->current_pins_numa -= shared_frame->numa_frame->pin_count.load();  // TODO: Remove this later
    _metrics->current_bytes_used_numa -= bytes;
    _numa_buffer_pools.deallocate_frame(shared_frame->numa_frame);
  }
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

BufferManager::Metrics BufferManager::metrics() {
  return *_metrics;
}

void BufferManager::reset_metrics() {
  _metrics = std::make_shared<Metrics>();
}

void BufferManager::clear() {
  _num_pages = 0;
  _dram_buffer_pools.clear();
  _numa_buffer_pools.clear();
  _ssd_region = std::make_shared<SSDRegion>(_ssd_region->get_file_name());
}

std::size_t BufferManager::numa_bytes_used() const {
  return _numa_buffer_pools._used_bytes.load();
}

std::size_t BufferManager::dram_bytes_used() const {
  return _dram_buffer_pools._used_bytes.load();
}

BufferManager& BufferManager::operator=(BufferManager&& other) {
  if (&other != this) {
    _num_pages = other._num_pages.load();
    _dram_buffer_pools = std::move(other._dram_buffer_pools);
    _numa_buffer_pools = std::move(other._numa_buffer_pools);
    _ssd_region = other._ssd_region;
    _migration_policy = std::move(other._migration_policy);
    _metrics = other._metrics;
    _config = other._config;
  }
  return *this;
}

}  // namespace hyrise