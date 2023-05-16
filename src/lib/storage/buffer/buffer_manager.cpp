#include "buffer_manager.hpp"
#include <algorithm>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_ptr.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

//----------------------------------------------------
// Config
//----------------------------------------------------

BufferManager::Config BufferManager::Config::from_env() {
  if (const auto json_path = std::getenv("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH")) {
    std::ifstream json_file(json_path);
    if (!json_file.is_open()) {
      Fail("Failed to open HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH file");
    }

    nlohmann::json json;
    try {
      json_file >> json;
    } catch (const std::exception& e) {
      Fail("Failed to parse HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH file: " + std::string(e.what()));
    }

    auto config = BufferManager::Config{};
    config.dram_buffer_pool_size = json.value("dram_buffer_pool_size", config.dram_buffer_pool_size);
    config.numa_buffer_pool_size = json.value("numa_buffer_pool_size", config.numa_buffer_pool_size);

    if (std::filesystem::is_block_file(json.value("ssd_path", config.ssd_path))) {
      config.ssd_path = json.value("ssd_path", config.ssd_path);
    } else if (std::filesystem::is_directory(json.value("ssd_path", config.ssd_path))) {
      const auto path = std::filesystem::path(json.value("ssd_path", config.ssd_path));
      const auto now = std::chrono::system_clock::now();
      const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
      const auto db_file = std::filesystem::path{"hyrise-buffer-pool-" + std::to_string(timestamp) + ".bin"};
      config.ssd_path = path / db_file;
    } else {
      config.ssd_path = json.value("ssd_path", config.ssd_path);
    }
    auto migration_policy_json = json.value("migration_policy", nlohmann::json{});
    config.migration_policy = MigrationPolicy{
        migration_policy_json.value("dram_read_ratio", config.migration_policy.get_dram_read_ratio()),
        migration_policy_json.value("dram_write_ratio", config.migration_policy.get_dram_write_ratio()),
        migration_policy_json.value("numa_read_ratio", config.migration_policy.get_numa_read_ratio()),
        migration_policy_json.value("numa_write_ratio", config.migration_policy.get_numa_write_ratio())};
    config.enable_eviction_purge_worker =
        json.value("enable_eviction_purge_worker", config.enable_eviction_purge_worker);
    config.numa_memory_node = json.value("numa_memory_node", config.numa_memory_node);
    config.mode = magic_enum::enum_cast<BufferManagerMode>(json.value("mode", magic_enum::enum_name(config.mode)))
                      .value_or(config.mode);
    return config;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH not found in environment");
  }
}

//----------------------------------------------------
// Buffer Pools
//----------------------------------------------------

BufferManager::BufferPools::BufferPools(const PageType page_type, const size_t pool_size,
                                        const std::function<void(const FramePtr&)> evict_frame,
                                        const bool enable_eviction_purge_worker,
                                        const std::shared_ptr<BufferManager::Metrics> metrics)
    : _used_bytes(0),
      enabled(_page_type != PageType::Invalid),
      _evict_frame(evict_frame),
      _max_bytes(pool_size),
      _page_type(page_type),

      _metrics(metrics) {
  if (enabled) {
    _eviction_queue = std::make_shared<EvictionQueue>();
    _buffer_pools = create_volatile_regions_for_size_types(page_type, pool_size);
    if (enable_eviction_purge_worker) {
      _eviction_purge_worker = std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE,
                                                                    [&](size_t) { this->purge_eviction_queue(); });
    }
  }
}

BufferManager::BufferPools& BufferManager::BufferPools::operator=(BufferManager::BufferPools&& other) {
  _used_bytes = other._used_bytes.load();
  _max_bytes = other._max_bytes;
  _page_type = other._page_type;
  _page_type = other._page_type;
  // _evict_frame = other._evict_frame;
  enabled = other.enabled;
  _metrics = other._metrics;
  // Assert(_max_bytes == other._max_bytes, "Cannot move buffer pools with different sizes");
  // Assert(_page_type == other._page_type, "Cannot move buffer pools with different page types");
  _buffer_pools = std::move(other._buffer_pools);
  _eviction_purge_worker = std::move(other._eviction_purge_worker);
  _eviction_queue = std::move(other._eviction_queue);

  return *this;
}

// TODO: Track purges, verifiy proper eviction, rename to evict until free
void BufferManager::BufferPools::allocate_frame(FramePtr& frame) {
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");
  const auto required_size_type = frame->size_type;

  // This potentially overshoots the _used_bytes to reserves space for the new frame.
  // If this happens concurrently, we just continue evicting in each thread until we have enough space.
  const auto bytes_required = bytes_for_size_type(required_size_type);
  auto freed_bytes = size_t{0};
  auto current_bytes = _used_bytes.fetch_add(bytes_required);

  // Find potential victim frame if we don't have enough space left
  auto item = EvictionItem{};
  // TODO: Really need to convert to int64?
  while (((int64_t)current_bytes + (int64_t)bytes_required - (int64_t)freed_bytes) > (int64_t)_max_bytes) {
    if (!_eviction_queue->try_pop(item)) {
      _used_bytes -= bytes_required;
      Fail(
          "Cannot pop item from queue. All frames seems to be pinned. Please increase the memory size of this buffer "
          "pool.");
    }

    auto current_frame = item.frame;

    // Lock the frame to prevent it from being modified while we are working on it
    std::lock_guard<std::mutex> lock(current_frame->latch);
    DebugAssert(current_frame->page_type == _page_type, "Frame has wrong page type");

    // Check if the frame can be evicted based on the timestamp, the state and the pin count
    if (!item.can_evict()) {
      continue;
    }

    // Check if the frame is second chance evictable. If not, just requeue it and remove the reference bit to give it another chance.
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
      // TODO: Lock frame?
      _buffer_pools[static_cast<size_t>(required_size_type)]->move(current_frame, frame);
      break;
    } else {
      // Otherwise, release physical pages of the current frame and continue evicting
      _buffer_pools[static_cast<size_t>(current_frame_size_type)]->free(current_frame);
      if (_page_type == PageType::Dram) {
        _metrics->num_madvice_free_calls_dram++;
      } else if (_page_type == PageType::Numa) {
        _metrics->num_madvice_free_calls_numa++;
      } else {
        Fail("Invalid page type");
      }
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

  add_to_eviction_queue(frame);
  frame->set_resident();
}

void BufferManager::BufferPools::deallocate_frame(FramePtr& frame) {
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
    if (!item.can_evict()) {
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

void BufferManager::BufferPools::add_to_eviction_queue(const FramePtr& frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->page_type == _page_type, "Frame has wrong page type");

  // Insert the frame into the eviction queue with an updated timestamp
  const auto timestamp_before_eviction = frame->eviction_timestamp++;
  _eviction_queue->push({frame, timestamp_before_eviction + 1});
  frame->set_referenced();

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
                         config.enable_eviction_purge_worker, _metrics),
      _numa_buffer_pools(page_type_for_numa_buffer_pools(config.mode), config.numa_buffer_pool_size,
                         std::bind(&BufferManager::evict_frame, this, std::placeholders::_1),
                         config.enable_eviction_purge_worker, _metrics) {
  Assert(_dram_buffer_pools.enabled || _numa_buffer_pools.enabled, "No Buffer Pool is enabled");
}

BufferManager::Config BufferManager::get_config() const {
  return _config;
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

FramePtr BufferManager::make_resident(FramePtr frame, const AccessIntent access_intent) {
  // TODO: Retry mechanism for failed allocations or for optimistic locking

  auto dram_frame = frame->page_type == PageType::Dram ? frame : frame->sibling_frame;
  const auto is_dram_resident = dram_frame != nullptr && dram_frame->is_resident();

  // Case 1: We passed a DRAM frame and it is already resident. This is the easy case.
  // Case 2: We passed a NUMA frame and it has a sibling DRAM frame that is already resident. This is basically the same as case 1.
  if (is_dram_resident) {
    dram_frame->set_referenced();
    return dram_frame;
  }

  auto numa_frame = frame->page_type == PageType::Numa ? frame : frame->sibling_frame;
  const auto is_numa_resident = numa_frame != nullptr && numa_frame->is_resident();

  // Case 3: The frame is not resident in DRAM at all, we need to decide if we want to bypass DRAM or not
  if (_numa_buffer_pools.enabled) {
    DebugAssert(dram_frame != numa_frame, "Both frames are the same");
    // Check if we want to bypass DRAM and use the NUMA frame directly
    const auto bypass_dram = (access_intent == AccessIntent::Read && _migration_policy.bypass_dram_during_read()) ||
                             (access_intent == AccessIntent::Write && _migration_policy.bypass_dram_during_write());

    if (is_numa_resident && bypass_dram) {
      numa_frame->set_referenced();
      return numa_frame;
    } else if (!is_numa_resident && bypass_dram) {
      const auto bypass_numa = (access_intent == AccessIntent::Read && _migration_policy.bypass_numa_during_read()) ||
                               (access_intent == AccessIntent::Write && _migration_policy.bypass_numa_during_write());

      if (!bypass_numa) {
        if (!numa_frame) {
          numa_frame = dram_frame->clone_and_attach_sibling<PageType::Numa>();
        }
        std::lock_guard<std::mutex> lock(numa_frame->latch);

        DebugAssert(!numa_frame->is_resident(), "DRAM frame is already resident");
        _numa_buffer_pools.allocate_frame(numa_frame);

        numa_frame->pin();
        read_page_from_ssd(numa_frame);
        numa_frame->dirty = true;
        numa_frame->unpin();

        _numa_buffer_pools.add_to_eviction_queue(numa_frame);

        return numa_frame;
      }
      // Else is handled with Case 4
    } else if (is_numa_resident && !bypass_dram) {
      if (!dram_frame) {
        dram_frame = numa_frame->clone_and_attach_sibling<PageType::Dram>();
      }
      _dram_buffer_pools.allocate_frame(dram_frame);

      numa_frame->wait_until_unpinned();

      DebugAssert(!dram_frame->is_pinned(), "DRAM frame is already pinned");
      std::scoped_lock lock(numa_frame->latch, dram_frame->latch);

      numa_frame->pin();
      dram_frame->pin();

      numa_frame->copy_data_to(dram_frame);

      dram_frame->dirty = true;
      dram_frame->unpin();
      numa_frame->unpin();

      _dram_buffer_pools.add_to_eviction_queue(dram_frame);
      _numa_buffer_pools.add_to_eviction_queue(numa_frame);

      return dram_frame;
    }
    // !is_numa_resident && !bypass_dram is also handled with Case 4
  }

  // Case 4: Neither in DRAM nor in NUMA or not bypass DRAM --> we need to bring the page into DRAM
  DebugAssert(dram_frame, "DRAM frame is not allocated");

  std::lock_guard<std::mutex> lock(dram_frame->latch);

  dram_frame->pin();
  _dram_buffer_pools.allocate_frame(dram_frame);
  read_page_from_ssd(dram_frame);
  dram_frame->dirty = true;
  dram_frame->unpin();
  _dram_buffer_pools.add_to_eviction_queue(dram_frame);

  return dram_frame;
}

void BufferManager::evict_frame(const FramePtr& frame) {
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->is_resident(), "Frame is not resident");
  DebugAssert(!frame->is_pinned(), "Frame is still pinned");
  DebugAssert(!frame->latch.try_lock(),
              "Frame should not be locked. Otherwise, we run into a deadlock when using this function as callback");

  // Case 1: The frame is not dirty and we don't need to do anything
  if (!frame->dirty) {
    frame->set_evicted();
    return;
  }

  // Case 2: Numa buffer pools are disabled anyways, we can just write to SSD
  if (!_numa_buffer_pools.enabled) {
    DebugAssert(frame->page_type == PageType::Dram, "Invalid page type");
    write_page_to_ssd(frame);
    frame->dirty = false;
    frame->set_evicted();
    return;
  }

  // Case 3: The frame is either a NUMA frame or a DRAM frame and we can bypass NUMA
  const auto bypass_numa = _migration_policy.bypass_numa_during_write();
  if (frame->page_type == PageType::Numa || (frame->page_type == PageType::Dram && bypass_numa)) {
    frame->pin();
    write_page_to_ssd(frame);
    frame->dirty = false;
    frame->set_evicted();
    frame->unpin();
    return;
  }

  // Case 4: The frame is a DRAM frame and we can't bypass NUMA. We need to copy the data to a NUMA frame
  DebugAssert(frame->page_type == PageType::Dram, "Invalid page type");
  auto numa_frame = frame->sibling_frame;
  if (!numa_frame) {
    numa_frame = frame->clone_and_attach_sibling<PageType::Numa>();
  }
  numa_frame->wait_until_unpinned();
  DebugAssert(!numa_frame->dirty.load(), "NUMA frame is dirty");

  std::lock_guard<std::mutex> lock(numa_frame->latch);
  numa_frame->pin();
  frame->pin();

  if (!numa_frame->is_resident()) {
    _numa_buffer_pools.allocate_frame(numa_frame);
  }

  frame->copy_data_to(numa_frame);

  numa_frame->unpin();
  frame->unpin();

  numa_frame->try_set_dirty(true);
  frame->dirty = false;
  frame->set_evicted();

  _numa_buffer_pools.add_to_eviction_queue(numa_frame);
}

void BufferManager::read_page_from_ssd(const FramePtr& frame) {
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

void BufferManager::write_page_to_ssd(const FramePtr& frame) {
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

FramePtr BufferManager::new_frame(const PageSizeType size_type) {
  const auto page_id = PageID{_num_pages++};

  // TODO: We could just defer this to the first time the page is evicted
  _ssd_region->allocate(page_id, size_type);

  const auto use_dram =
      !_numa_buffer_pools.enabled || (_dram_buffer_pools.enabled && !_migration_policy.bypass_dram_during_write());

  if (use_dram) {
    auto allocated_dram_frame = make_frame(page_id, size_type, PageType::Dram);
    _dram_buffer_pools.allocate_frame(allocated_dram_frame);
    return allocated_dram_frame;
  } else {
    auto allocated_numa_frame = make_frame(page_id, size_type, PageType::Numa);
    _numa_buffer_pools.allocate_frame(allocated_numa_frame);
    return allocated_numa_frame;
  }
}

void BufferManager::unpin(const FramePtr& frame, const bool dirty) {
  if (frame->page_type == PageType::Dram) {
    _metrics->current_pins_dram--;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->current_pins_numa--;
  } else {
    Fail("Invalid page type");
  }

  frame->try_set_dirty(dirty);

  // Add to eviction queue if the frame is not pinned anymore
  const auto unpinned = frame->unpin();
  if (unpinned) {
    if (frame->page_type == PageType::Dram) {
      _dram_buffer_pools.add_to_eviction_queue(frame);
    } else if (frame->page_type == PageType::Numa) {
      _numa_buffer_pools.add_to_eviction_queue(frame);
    } else {
      Fail("Invalid page type");
    }
  }
}

void BufferManager::pin(const FramePtr& frame) {
  if (!frame->is_resident()) {
    make_resident(frame, AccessIntent::Read);
  }
  DebugAssert(frame->is_resident(), "Frame should be resident");
  frame->set_referenced();
  frame->pin();

  if (frame->page_type == PageType::Dram) {
    _metrics->current_pins_dram++;
    _metrics->total_pins_dram++;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->current_pins_numa++;
    _metrics->total_pins_numa++;
  } else {
    Fail("Invalid page type");
  }
}

std::pair<FramePtr, std::ptrdiff_t> BufferManager::find_frame_and_offset(const void* ptr) {
  // TODO: Unswizzle might be called with end() pointer and therefore going to the next page.
  // or set referenced use add_to_victionqeue to add it to the end of the queue
  if (_dram_buffer_pools.enabled) {
    for (auto& buffer_pool : _dram_buffer_pools._buffer_pools) {
      const auto unswizzled = buffer_pool->find_frame_and_offset(ptr);
      if (unswizzled.first) {
        unswizzled.first->set_referenced();
        return unswizzled;
      }
    }
  }
  if (_numa_buffer_pools.enabled) {
    for (auto& buffer_pool : _numa_buffer_pools._buffer_pools) {
      const auto unswizzled = buffer_pool->find_frame_and_offset(ptr);
      if (unswizzled.first) {
        unswizzled.first->set_referenced();
        return unswizzled;
      }
    }
  }

  return std::make_pair(DummyFrame(), 0);
};

BufferPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  Assert(align <= PAGE_ALIGNMENT, "Alignment must be smaller than page size");

  const auto page_size_type = find_fitting_page_size_type(bytes);

  const auto frame = new_frame(page_size_type);

  if (frame->page_type == PageType::Dram) {
    _metrics->current_bytes_used_dram += bytes;
    _metrics->total_unused_bytes_dram += bytes_for_size_type(page_size_type) - bytes;
    _metrics->total_allocated_bytes_dram += bytes;
  } else if (frame->page_type == PageType::Numa) {
    _metrics->current_bytes_used_numa += bytes;
    _metrics->total_unused_bytes_numa += bytes_for_size_type(page_size_type) - bytes;
    _metrics->total_allocated_bytes_numa += bytes;
  } else {
    Fail("Invalid page type");
  }

  _metrics->num_allocs++;
  return BufferPtr<void>(frame, 0, typename BufferPtr<void>::AllocTag{});
}

void BufferManager::deallocate(BufferPtr<void> ptr, std::size_t bytes, std::size_t align) {
  //Fail("Not implemented");
  // _metrics->num_deallocs++;
  // auto shared_frame = ptr._shared_frame;

  // if (!shared_frame) {
  //   // TODO: This case happens with the MVCC data structures when running the filebased benchmark
  //   return;
  // }

  // // Deallocate the frames from their buffer pools and free some memory
  // if (shared_frame->dram_frame) {
  //   _metrics->current_pins_dram -= shared_frame->dram_frame->pin_count.load();  // TODO: Remove this later
  //   _metrics->current_bytes_used_dram -= bytes;
  //   _dram_buffer_pools.deallocate_frame(shared_frame->dram_frame);
  // }

  // if (shared_frame->numa_frame) {
  //   _metrics->current_pins_numa -= shared_frame->numa_frame->pin_count.load();  // TODO: Remove this later
  //   _metrics->current_bytes_used_numa -= bytes;
  //   _numa_buffer_pools.deallocate_frame(shared_frame->numa_frame);
  // }
}

std::shared_ptr<BufferManager::Metrics> BufferManager::metrics() {
  return _metrics;
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
  // TODO: Think about swap
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