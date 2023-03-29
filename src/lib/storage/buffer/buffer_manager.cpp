#include "buffer_manager.hpp"
#include <algorithm>
#include <chrono>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
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
                                        const bool enable_eviction_worker, const std::shared_ptr<SSDRegion> ssd_region,
                                        const std::shared_ptr<BufferManager::Metrics> metrics)
    : _used_bytes(0),
      enabled(_page_type != PageType::Invalid),
      _total_bytes(pool_size),
      _page_type(page_type),
      _ssd_region(std::move(ssd_region)),
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
  Assert(_total_bytes == other._total_bytes, "Cannot move buffer pools with different sizes");
  Assert(_page_type == other._page_type, "Cannot move buffer pools with different page types");
  _ssd_region = std::move(other._ssd_region);
  _eviction_queue = std::move(other._eviction_queue);
  _buffer_pools = std::move(other._buffer_pools);
  _eviction_worker = std::move(other._eviction_worker);

  return *this;
}

template <typename EvictionCallback>
Frame* BufferManager::BufferPools::allocate_frame(const PageSizeType size_type, EvictionCallback&& eviction_callback) {
  const auto bytes_required = bytes_for_size_type(size_type);
  auto freed_bytes = size_t{0};
  const auto current_bytes = _used_bytes.load();

  Frame* frame = nullptr;
  auto item = EvictionItem{};

  while ((current_bytes + bytes_required - freed_bytes) > _total_bytes) {
    if (!_eviction_queue->try_pop(item)) {
      Fail("Cannot pop item from queue. No way to alloc new memory.");
    }

    // If the timestamp does not match, the frame is too old and we just check the next one
    if (item.timestamp < item.frame->eviction_timestamp.load() || item.frame->pin_count.load() > 0) {
      continue;
    }

    // Check if the frame was already released earlier (e.g. when the page was removed)
    if (item.frame->shared_frame) {
      eviction_callback(item.frame);
    }

    // Write out the frame if it's dirty
    if (item.frame->dirty) {
      write_page(item.frame);  // TODO: Write to Numa or SSD?
      item.frame->dirty = false;
    }

    const auto frame_size_type = item.frame->size_type;
    freed_bytes += bytes_for_size_type(frame_size_type);

    if (frame_size_type == size_type) {
      // If the evicted frame is of the same page type, we should just use it directly.
      frame = item.frame;
      break;
    } else {
      // Otherwise, release physical pages the current frame and continue evicting
      _buffer_pools[static_cast<size_t>(frame_size_type)]->free(item.frame);
      _metrics->num_madvice_free_calls++;
    }
  }

  _used_bytes.fetch_add(bytes_required - freed_bytes);
  DebugAssert(_used_bytes.load() <= _total_bytes, "Used bytes exceeds total bytes");

  // If we did not find a frame of the correct size, we allocate a new one
  if (!frame) {
    frame = _buffer_pools[static_cast<size_t>(size_type)]->allocate();
    DebugAssert(frame, "Could not allocate frame");
  }
  frame->eviction_timestamp.store(0);

  return frame;
}

void BufferManager::BufferPools::purge_eviction_queue() {
  auto item = EvictionItem{};

  // Copy pointer to eviction queue to ensure access safety when task is running, but the buffer manager
  //is already destroyed
  auto eviction_queue = _eviction_queue;
  if (!eviction_queue) {
    return;
  }

  while (true) {
    // Check if the queue is empty
    if (!eviction_queue->try_pop(item)) {
      break;
    }

    // Remove old items with an outdated timestamp
    if (item.timestamp < item.frame->eviction_timestamp.load()) {
      if (_page_type == PageType::Dram) {
        _metrics->num_dram_eviction_queue_item_purges++;
      } else if (_page_type == PageType::Numa) {
        _metrics->num_numa_eviction_queue_item_purges++;
      } else {
        Fail("Invalid page type");
      }
      continue;
    }

    // Requeue the item
    eviction_queue->push(item);
  }

  if (_page_type == PageType::Dram) {
    _metrics->num_dram_eviction_queue_purges++;
  } else if (_page_type == PageType::Numa) {
    _metrics->num_numa_eviction_queue_purges++;
  } else {
    Fail("Invalid page type");
  }
}

void BufferManager::BufferPools::add_to_eviction_queue(Frame* frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  // Insert the frame into the eviction queue with an updated timestamp
  const auto timestamp_before_eviction = frame->eviction_timestamp++;
  _eviction_queue->push({frame, timestamp_before_eviction + 1});

  _metrics->num_dram_eviction_queue_adds++;
}

void BufferManager::BufferPools::read_page(Frame* frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  _ssd_region->read_page(frame->page_id, frame->size_type, frame->data);
  _metrics->num_ssd_reads++;
  _metrics->total_bytes_read_from_ssd += bytes_for_size_type(frame->size_type);
}

void BufferManager::BufferPools::write_page(const Frame* frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  _ssd_region->write_page(frame->page_id, frame->size_type, frame->data);
  _metrics->num_ssd_writes++;
  _metrics->total_bytes_written_to_ssd += bytes_for_size_type(frame->size_type);
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
      _migration_policy(config.migration_policy),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path)),
      _metrics(std::make_shared<Metrics>()),
      _dram_buffer_pools(page_type_for_dram_buffer_pools(config.mode), config.dram_buffer_pool_size,
                         config.enable_eviction_worker, _ssd_region, _metrics),
      _numa_buffer_pools(page_type_for_numa_buffer_pools(config.mode), config.numa_buffer_pool_size,
                         config.enable_eviction_worker, _ssd_region, _metrics) {
  // if constexpr (HYRISE_DEBUG) {
  //   std::cout << "Init Buffer Manager (" << this << ") - " << std::endl;
  // }
}

BufferManager::~BufferManager() {
  // if constexpr (HYRISE_DEBUG) {
  //   std::cout << "Destroy Buffer Manager (" << this << ")" << std::endl;
  // }
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

Frame* BufferManager::get_frame(const PageID page_id, const PageSizeType size_type) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto shared_frame = find_in_page_table(page_id);
  if (shared_frame) {
    if (_dram_buffer_pools.enabled && shared_frame->dram_frame) {
      // 1. Found the frame in DRAM
      return shared_frame->dram_frame;
    } else if (_numa_buffer_pools.enabled && shared_frame->numa_frame) {
      // 2. Found the frame on NUMA and we want to migrate it to DRAM TODO: read or write intent?!
      const auto bypass_dram = _migration_policy.bypass_dram_during_read();
      if (bypass_dram) {
        // 3. We want to bypass dram
        return shared_frame->numa_frame;
      }

      // 4. Lets not bypass dram and init a new DRAM frame
      auto allocated_dram_frame = _dram_buffer_pools.allocate_frame(
          size_type, std::bind(&BufferManager::remove_frame, this, std::placeholders::_1));
      allocated_dram_frame->init(page_id);

      // TODO: Allocate latches for the frame
      std::memcpy(allocated_dram_frame->data, shared_frame->numa_frame->data, bytes_for_size_type(size_type));
      shared_frame->link_dram_frame(allocated_dram_frame);
      return allocated_dram_frame;
    } else {
      Fail("Found a page in the page table that is neither in DRAM nor on NUMA.");
    }
  } else {
    // 5. The page is neither in DRAM nor on NUMA, we need to load it from SSD
    auto allocated_dram_frame = _dram_buffer_pools.allocate_frame(
        size_type, std::bind(&BufferManager::remove_frame, this, std::placeholders::_1));

    // Update the frame metadata and read the page data
    allocated_dram_frame->init(page_id);

    // We make a full read from SSD
    _dram_buffer_pools.read_page(allocated_dram_frame);

    // Update the page table and metadata
    {
      std::lock_guard<std::mutex> lock(_page_table_mutex);
      _page_table[page_id] = std::make_shared<SharedFrame>(allocated_dram_frame);
    }

    return allocated_dram_frame;
  }
}

void BufferManager::remove_frame(Frame* frame) {
  // TODO: This can be templated potentially
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->shared_frame != nullptr, "Shared frame is null");

  DebugAssert(frame->page_id != INVALID_PAGE_ID, "Page ID is invalid");

  // TODO: We might need a lock
  auto shared_frame = frame->shared_frame;

  if (frame->page_type == PageType::Dram) {
    shared_frame->dram_frame = nullptr;
  } else if (frame->page_type == PageType::Numa) {
    shared_frame->numa_frame = nullptr;
  } else {
    Fail("Invalid page type");
  }
  frame->shared_frame = nullptr;

  // Remove the frame from the page table if no frame is left
  if (!shared_frame->dram_frame && !shared_frame->numa_frame) {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table.erase(frame->page_id);
  }
}

std::byte* BufferManager::get_page(const PageID page_id, const PageSizeType size_type) {
  return get_frame(page_id, size_type)->data;
}

std::shared_ptr<SharedFrame> BufferManager::find_in_page_table(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  std::lock_guard<std::mutex> lock(_page_table_mutex);

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    _metrics->page_table_hits++;
    auto [_, frame] = *frame_in_page_table_it;
    return frame;
  }
  _metrics->page_table_misses++;
  return nullptr;
}

Frame* BufferManager::new_page(const PageSizeType size_type) {
  auto frame = _dram_buffer_pools.allocate_frame(size_type,
                                                 std::bind(&BufferManager::remove_frame, this, std::placeholders::_1));
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->data != nullptr, "Frame data is null");
  const auto page_id = _num_pages++;

  // Update the frame metadata
  frame->page_id = page_id;
  frame->dirty = true;
  frame->pin_count.store(0);

  _ssd_region->allocate(frame->page_id, size_type);

  // Add the frame to the page table with lock guard in new scope
  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table[frame->page_id] = std::make_shared<SharedFrame>(frame);
  }

  _dram_buffer_pools.add_to_eviction_queue(frame);

  return frame;
}

void BufferManager::unpin_page(const PageID page_id, const PageSizeType size_type, const bool dirty) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    // Fail("Cannot unpin a page that is not in the page table. Check if the PinGuard is properly used.");
    return;
  }

  const auto dram_frame = frame->dram_frame;
  // DebugAssert(frame->pin_count.load() > 0, "Cannot unpin a page that is not pinned");

  dram_frame->dirty = dram_frame->dirty.load() || dirty;
  dram_frame->pin_count--;
  if (dram_frame->pin_count.load() == 0) {
    _dram_buffer_pools.add_to_eviction_queue(dram_frame);
  }
}

void BufferManager::pin_page(const PageID page_id, const PageSizeType size_type) {
  auto frame = get_frame(page_id, size_type);

  frame->pin_count++;
}

uint32_t BufferManager::get_pin_count(const PageID page_id) {
  const auto shared_frame = find_in_page_table(page_id);
  if (shared_frame == nullptr) {
    return 0;
  }
  if (shared_frame->dram_frame) {
    return shared_frame->dram_frame->pin_count.load();
  }

  if (shared_frame->numa_frame) {
    return shared_frame->numa_frame->pin_count.load();
  }

  return 0;
}

bool BufferManager::is_dirty(const PageID page_id) {
  const auto shared_frame = find_in_page_table(page_id);
  if (shared_frame == nullptr) {
    return false;
  }
  if (shared_frame->dram_frame) {
    return shared_frame->dram_frame->dirty.load();
  }

  if (shared_frame->numa_frame) {
    return shared_frame->numa_frame->dirty.load();
  }

  return false;
}

void BufferManager::remove_page(const PageID page_id) {
  // TODO: If else here
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  auto dram_frame = frame->dram_frame;

  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table.erase(dram_frame->page_id);
  }

  // TODO: DebugAssert(!frame->pin_count, "Page cannot be removed while pinned. This can be used to debug resizes.");

  dram_frame->dirty = false;
  dram_frame->eviction_timestamp.store(0);
  dram_frame->pin_count.store(0);

  _dram_buffer_pools.add_to_eviction_queue(dram_frame);
}

std::pair<Frame*, std::ptrdiff_t> BufferManager::unswizzle(const void* ptr) {
  if (_dram_buffer_pools.enabled) {
    for (auto& buffer_pool : _dram_buffer_pools._buffer_pools) {
      if (const auto frame = buffer_pool->unswizzle(ptr)) {
        return std::make_tuple(frame, reinterpret_cast<const std::byte*>(ptr) - frame->data);
      }
    }
  }
  if (_numa_buffer_pools.enabled) {
    for (auto& buffer_pool : _numa_buffer_pools._buffer_pools) {
      if (const auto frame = buffer_pool->unswizzle(ptr)) {
        return std::make_tuple(frame, reinterpret_cast<const std::byte*>(ptr) - frame->data);
      }
    }
  }

  return std::make_tuple(nullptr, 0);
};

BufferManagedPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  const auto page_size_type = find_fitting_page_size_type(bytes);

  // Update metrics
  _metrics->current_bytes_used += bytes;
  _metrics->total_unused_bytes += bytes_for_size_type(page_size_type) - bytes;
  _metrics->max_bytes_used = std::max(_metrics->max_bytes_used, _metrics->current_bytes_used);
  _metrics->total_allocated_bytes += bytes;
  _metrics->num_allocs++;

  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto frame = new_page(page_size_type);

  return BufferManagedPtr<void>(this, frame->page_id, 0, frame->size_type);
}

void BufferManager::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  remove_page(ptr.get_page_id());

  _metrics->current_bytes_used -= bytes;
  // TODO: May be missing some metrics
  _metrics->max_bytes_used = std::max(_metrics->max_bytes_used, _metrics->current_bytes_used);
  _metrics->num_deallocs++;
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

BufferManager::Metrics BufferManager::metrics() {
  return *_metrics;
}

void BufferManager::clear() {
  _num_pages = 0;
  _page_table = {};
  _dram_buffer_pools.clear();
  _numa_buffer_pools.clear();
  _ssd_region = std::make_shared<SSDRegion>(_ssd_region->get_file_name());
}

void BufferManager::flush_all_pages() {
  std::lock_guard<std::mutex> lock(_page_table_mutex);

  // TODO: Acquire locks?
  for (auto [page_id, shared_frame] : _page_table) {
    if (shared_frame->dram_frame) {
      Assert(shared_frame->dram_frame->pin_count == 0, "Cannot flush page while pinned.");
      if (shared_frame->dram_frame->dirty) {
        _dram_buffer_pools.write_page(shared_frame->dram_frame);
      }
      _dram_buffer_pools.clear();
    }

    if (shared_frame->numa_frame) {
      Assert(shared_frame->numa_frame->pin_count == 0, "Cannot flush page while pinned.");
      if (shared_frame->numa_frame->dirty) {
        _numa_buffer_pools.write_page(shared_frame->numa_frame);
      }
      _numa_buffer_pools.clear();
    }
  }

  _page_table.clear();
}

std::size_t BufferManager::numa_bytes_used() const {
  return _numa_buffer_pools._used_bytes.load();
}

std::size_t BufferManager::dram_bytes_used() const {
  return _dram_buffer_pools._used_bytes.load();
}

BufferManager& BufferManager::operator=(BufferManager&& other) {
  if (&other != this) {
    std::unique_lock this_lock(_page_table_mutex, std::defer_lock);
    std::unique_lock other_lock(other._page_table_mutex, std::defer_lock);
    std::lock(this_lock, other_lock);

    _num_pages = other._num_pages.load();
    _page_table = std::move(other._page_table);
    _dram_buffer_pools = std::move(other._dram_buffer_pools);
    _numa_buffer_pools = std::move(other._numa_buffer_pools);
    _ssd_region = std::move(other._ssd_region);
    _metrics = other._metrics;
  }
  return *this;
}

}  // namespace hyrise