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

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> make_dram_buffer_pools(const size_t num_bytes) {
  return {
      std::make_unique<VolatileRegion>(PageSizeType::KiB8, num_bytes),
      std::make_unique<VolatileRegion>(PageSizeType::KiB16, num_bytes),
      std::make_unique<VolatileRegion>(PageSizeType::KiB32, num_bytes),
      std::make_unique<VolatileRegion>(PageSizeType::KiB64, num_bytes),
      std::make_unique<VolatileRegion>(PageSizeType::KiB128, num_bytes),
      std::make_unique<VolatileRegion>(PageSizeType::KiB256, num_bytes),
  };
}

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
                                        const bool enable_eviction_worker, const std::shared_ptr<SSDRegion> ssd_region)
    : _used_bytes(0),
      _total_bytes(pool_size),
      _page_type(page_type),
      _ssd_region(std::move(ssd_region)),
      _eviction_queue(std::make_shared<EvictionQueue>()),
      _buffer_pools(make_dram_buffer_pools(pool_size)) {
  Assert(_page_type != PageType::Invalid, "Invalid page type");
  if (enable_eviction_worker) {
    _eviction_worker =
        std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE, [&](size_t) { this->purge_eviction_queue(); });
  }
}

BufferManager::BufferPools& BufferManager::BufferPools::operator=(BufferManager::BufferPools&& other) {
  _used_bytes = other._used_bytes.load();
  // _total_bytes = other._total_bytes;
  // _page_type = other._page_type;
  _ssd_region = std::move(other._ssd_region);
  _eviction_queue = std::move(other._eviction_queue);
  _buffer_pools = std::move(other._buffer_pools);
  _eviction_worker = std::move(other._eviction_worker);
  return *this;
}

Frame* BufferManager::BufferPools::allocate_frame(const PageSizeType size_type) {
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

    // Remove from page table
    {
      // TODO: Lock this!!
      if (_page_type == PageType::Dram) {
        item.frame->shared_frame->dram_frame = nullptr;
      } else if (_page_type == PageType::Numa) {
        item.frame->shared_frame->numa_frame = nullptr;
      }
      item.frame->shared_frame = nullptr;
    }

    // Write out the frame if it's dirty
    if (item.frame->dirty) {
      write_page(item.frame);
      item.frame->dirty = false;
    }

    // TODO: Acquire Page Lock for frame

    const auto frame_size_type = item.frame->size_type;
    freed_bytes += bytes_for_size_type(frame_size_type);

    if (frame_size_type == size_type) {
      // If the evicted frame is of the same page type, we should just use it directly.
      frame = item.frame;
      break;
    } else {
      // Otherwise, Unmap the current frame and continue evicting
      _buffer_pools[static_cast<size_t>(frame_size_type)]->free(item.frame);
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
      // TODO: _metrics.num_eviction_queue_item_purges++;
      continue;
    }

    // Requeue the item
    eviction_queue->push(item);
  }
  // TODO: _metrics.num_eviction_queue_purges++;
}

void BufferManager::BufferPools::add_to_eviction_queue(Frame* frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  // Insert the frame into the eviction queue with an updated timestamp
  const auto timestamp_before_eviction = frame->eviction_timestamp++;
  _eviction_queue->push({frame, timestamp_before_eviction + 1});

  // TODO: _metrics.num_eviction_queue_adds++;
}

void BufferManager::BufferPools::read_page(Frame* frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  _ssd_region->read_page(frame->page_id, frame->size_type, frame->data);
  // TODO: _metrics.total_bytes_read += bytes_for_size_type(frame->size_type);
}

void BufferManager::BufferPools::write_page(const Frame* frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  _ssd_region->write_page(frame->page_id, frame->size_type, frame->data);
  // TODO:_metrics.total_bytes_written += bytes_for_size_type(frame->size_type);
}

//----------------------------------------------------
// Buffer Manager
//----------------------------------------------------

BufferManager::BufferManager(const Config config)
    : _num_pages(0),
      _migration_policy(config.migration_policy),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path)),
      _dram_buffer_pools(PageType::Dram, config.dram_buffer_pool_size, config.enable_eviction_worker, _ssd_region) {
  Assert(config.mode == BufferManagerMode::DramSSD, "Only DRAM/SSD mode is supported at the moment");
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

Frame* BufferManager::get_frame(const PageID page_id, const PageSizeType size_type) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  if (const auto frame = find_in_page_table(page_id)) {
    return frame->dram_frame;
  }
  auto allocated_frame = _dram_buffer_pools.allocate_frame(size_type);

  // Update the frame metadata and read the page data
  allocated_frame->page_id = page_id;
  allocated_frame->dirty = false;
  allocated_frame->pin_count.store(0);
  // TODO: This is a bit ugly
  _dram_buffer_pools.read_page(allocated_frame);

  // Update the page table and metadata
  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table[allocated_frame->page_id] = std::make_shared<SharedFrame>(allocated_frame);
  }
  return allocated_frame;
}

std::byte* BufferManager::get_page(const PageID page_id, const PageSizeType size_type) {
  return get_frame(page_id, size_type)->data;
}

std::shared_ptr<SharedFrame> BufferManager::find_in_page_table(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  std::lock_guard<std::mutex> lock(_page_table_mutex);

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    _metrics.page_table_hits++;
    auto [_, frame] = *frame_in_page_table_it;
    return frame;
  }
  _metrics.page_table_misses++;
  return nullptr;
}

Frame* BufferManager::new_page(const PageSizeType size_type) {
  auto frame = _dram_buffer_pools.allocate_frame(size_type);
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->data != nullptr, "Frame data is null");

  // Update the frame metadata
  frame->page_id = _num_pages;
  frame->dirty = true;
  frame->pin_count.store(0);

  _ssd_region->allocate(frame->page_id, size_type);

  // Add the frame to the page table with lock guard in new scope
  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table[frame->page_id] = std::make_shared<SharedFrame>(frame);
  }

  _dram_buffer_pools.add_to_eviction_queue(frame);

  _num_pages++;

  return frame;
}

void BufferManager::unpin_page(const PageID page_id, const PageSizeType size_type, const bool dirty) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    Fail("Cannot unpin a page that is not in the page table. Check if the PinGuard is properly used.");
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
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return 0;
  }
  return frame->dram_frame->pin_count.load();
}

bool BufferManager::is_dirty(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return false;
  }
  return frame->dram_frame->dirty.load();
}

void BufferManager::remove_page(const PageID page_id) {
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

std::tuple<PageID, PageSizeType, std::ptrdiff_t> BufferManager::unswizzle(const void* ptr) {
  for (auto& buffer_pool : _dram_buffer_pools._buffer_pools) {
    if (const auto frame = buffer_pool->unswizzle(ptr)) {
      return std::make_tuple(frame->page_id, frame->size_type, reinterpret_cast<const std::byte*>(ptr) - frame->data);
    }
  }

  return std::make_tuple(INVALID_PAGE_ID, PageSizeType::KiB8, 0);
};

BufferManagedPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  const auto page_size_type = find_fitting_page_size_type(bytes);

  // Update metrics
  _metrics.current_bytes_used += bytes;
  _metrics.total_unused_bytes += bytes_for_size_type(page_size_type) - bytes;
  _metrics.max_bytes_used = std::max(_metrics.max_bytes_used, _metrics.current_bytes_used);
  _metrics.total_allocated_bytes += bytes;
  _metrics.num_allocs++;

  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto frame = new_page(page_size_type);

  return BufferManagedPtr<void>(this, frame->page_id, 0, frame->size_type);
}

void BufferManager::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  remove_page(ptr.get_page_id());

  _metrics.current_bytes_used -= bytes;
  _metrics.max_bytes_used = std::max(_metrics.max_bytes_used, _metrics.current_bytes_used);
  _metrics.num_deallocs++;
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

BufferManager::Metrics BufferManager::metrics() {
  return _metrics;
}

std::shared_ptr<SSDRegion> BufferManager::get_ssd_region() {
  return _ssd_region;
}

void BufferManager::clear() {
  _num_pages = 0;
  _page_table = {};
  // _used_bytes = 0; TODO
  // _eviction_queue->clear();
  // for (auto& pool : _dram_buffer_pools) {
  //   pool->clear();
  // }
  _ssd_region = std::make_shared<SSDRegion>(_ssd_region->get_file_name());
}

BufferManager& BufferManager::operator=(BufferManager&& other) {
  if (&other != this) {
    std::unique_lock this_lock(_page_table_mutex, std::defer_lock);
    std::unique_lock other_lock(other._page_table_mutex, std::defer_lock);
    std::lock(this_lock, other_lock);

    _num_pages = other._num_pages.load();
    _page_table = std::move(other._page_table);
    _dram_buffer_pools = std::move(other._dram_buffer_pools);
    _ssd_region = std::move(other._ssd_region);
    _metrics = other._metrics;
  }
  return *this;
}

}  // namespace hyrise