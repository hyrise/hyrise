#include "buffer_manager.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <nlohmann/json.hpp>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

// TODO: On Mac, we should use MSYNC to see if a page is still in memory or not to avoid loading from disk
// TODO: Incluse page size in mihration desction -> large page size should be normalized

//----------------------------------------------------
// Helper Functions for Memory Mapping and Yielding
//----------------------------------------------------

std::byte* create_mapped_region() {
  Assert(bytes_for_size_type(MIN_PAGE_SIZE_TYPE) >= get_os_page_size(),
         "Smallest page size does not fit into an OS page: " + std::to_string(get_os_page_size()));
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  const auto mapped_memory =
      static_cast<std::byte*>(mmap(NULL, DEFAULT_RESERVED_VIRTUAL_MEMORY, PROT_READ | PROT_WRITE, flags, -1, 0));

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(errno));
  }

  return mapped_memory;
}

std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(
    std::byte* mapped_region, std::shared_ptr<BufferManagerMetrics> metrics) {
  DebugAssert(mapped_region != nullptr, "Region not properly mapped");
  auto array = std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>{};

  // Ensure that every region has the same amount of virtual memory
  // Round to the next multiple of the largest page size
  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; i++) {
    array[i] = std::make_shared<VolatileRegion>(
        magic_enum::enum_value<PageSizeType>(i), mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * i,
        mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * (i + 1), metrics);
  }

  return array;
}

void unmap_region(std::byte* region) {
  if (munmap(region, DEFAULT_RESERVED_VIRTUAL_MEMORY) < 0) {
    const auto error = errno;
    Fail("Failed to unmap volatile pool region: " + strerror(errno));
  }
}

void yield() {
  // TODO: Upgrade yield based on counter with some kind of exponential backoff
  std::this_thread::yield();
};

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

    if (std::filesystem::is_block_file(json.value("ssd_path", config.ssd_path)) ||
        std::filesystem::is_directory(json.value("ssd_path", config.ssd_path))) {
      config.ssd_path = json.value("ssd_path", config.ssd_path);
    } else {
      Fail("ssd_path is neither a block device nor a directory");
    }
    auto migration_policy_json = json.value("migration_policy", nlohmann::json{});
    config.migration_policy = MigrationPolicy{
        migration_policy_json.value("dram_read_ratio", config.migration_policy.get_dram_read_ratio()),
        migration_policy_json.value("dram_write_ratio", config.migration_policy.get_dram_write_ratio()),
        migration_policy_json.value("numa_read_ratio", config.migration_policy.get_numa_read_ratio()),
        migration_policy_json.value("numa_write_ratio", config.migration_policy.get_numa_write_ratio())};
    config.enable_eviction_purge_worker =
        json.value("enable_eviction_purge_worker", config.enable_eviction_purge_worker);
    config.memory_node =
        static_cast<NumaMemoryNode>(json.value("memory_node", static_cast<int8_t>(config.memory_node)));
    return config;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH not found in environment");
  }
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

BufferManager::BufferManager(const Config config)
    : _config(config),
      _mapped_region(create_mapped_region()),
      _metrics(std::make_shared<BufferManagerMetrics>()),
      _volatile_regions(create_volatile_regions(_mapped_region, _metrics)),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path, _metrics)),  // TODO: imprive init of pools here
      _primary_buffer_pool(std::make_shared<BufferManager::BufferPool>(
          config.dram_buffer_pool_size, config.enable_eviction_purge_worker, _volatile_regions, config.migration_policy,
          _ssd_region, config.memory_node == NO_NUMA_MEMORY_NODE ? nullptr : _secondary_buffer_pool, _metrics,
          DEFAULT_DRAM_NUMA_NODE)),
      _secondary_buffer_pool(std::make_shared<BufferManager::BufferPool>(
          config.dram_buffer_pool_size, config.enable_eviction_purge_worker, _volatile_regions, config.migration_policy,
          _ssd_region, nullptr, _metrics, config.memory_node)) {}

BufferManager::~BufferManager() {
  unmap_region(_mapped_region);
}

BufferManager& BufferManager::operator=(BufferManager&& other) noexcept {
  if (&other != this) {
    _metrics = other._metrics;
    _config = std::move(other._config);
    _volatile_regions = other._volatile_regions;
    _ssd_region = other._ssd_region;
    _primary_buffer_pool = other._primary_buffer_pool;
    _secondary_buffer_pool = other._secondary_buffer_pool;
    std::swap(_mapped_region, other._mapped_region);
  }
  return *this;
}

BufferManager& BufferManager::get() {
  return Hyrise::get().buffer_manager;
}

// TODO: This can take several templates to improve branching
void BufferManager::make_resident(const PageID page_id, const AccessIntent access_intent,
                                  const StateVersionType state_before_exclusive) {
  // Check if the page was freshly allocated by checking the version. In this case, we want to use either DRAM or NUMA
  const auto version = Frame::version(state_before_exclusive);
  const auto is_evicted = Frame::state(state_before_exclusive) == Frame::EVICTED;

  // Case 1: The page is already on DRAM. This is the easy case.
  if (!is_evicted && Frame::memory_node(state_before_exclusive) == _primary_buffer_pool->memory_node) {
    return;
  }

  auto region = get_region(page_id);
  const auto is_new_page = version == 0;

  // Case 2: The page was freshly allocated and is not on DRAM yet. We can just decide if it should be on numa or dram
  // and we dont want to load anything from SSD
  if (is_new_page) {
    DebugAssert(access_intent == AccessIntent::Write, "New pages should only be written to");
    unprotect_page(page_id);
    if (_secondary_buffer_pool->enabled() && !_config.migration_policy.bypass_numa_during_write()) {
      // Case 0.1: Use NUMA
      _secondary_buffer_pool->ensure_free_pages(page_id.size_type());
      region->move_to_numa_node(page_id, _secondary_buffer_pool->memory_node);
    } else {
      // Case 0.2: Use DRAM
      _primary_buffer_pool->ensure_free_pages(page_id.size_type());
      region->move_to_numa_node(page_id, _primary_buffer_pool->memory_node);
    }
    return;
  }

  if (!_secondary_buffer_pool->enabled()) {
    // Case 3: The page is not on DRAM and we don't have it on another memory node, so we need to load it from SSD
    unprotect_page(page_id);
    DebugAssert(Frame::memory_node(state_before_exclusive) == _primary_buffer_pool->memory_node, "Not on DRAM node");
    _primary_buffer_pool->ensure_free_pages(page_id.size_type());
    _ssd_region->read_page(page_id, region->get_page(page_id));
    _metrics->total_bytes_copied_from_ssd_to_dram.fetch_add(bytes_for_size_type(page_id.size_type()),
                                                            std::memory_order_relaxed);
    return;
  }

  // Case 4: The page is evicted anyways, decide if numa should be bypassed or not and load the page
  if (is_evicted) {
    const auto bypass_numa =
        (access_intent == AccessIntent::Read && _config.migration_policy.bypass_numa_during_read()) ||
        (access_intent == AccessIntent::Write && _config.migration_policy.bypass_numa_during_write());
    unprotect_page(page_id);
    if (bypass_numa) {
      // Case 4.1: We bypass NUMA and load directly into DRAM
      _primary_buffer_pool->ensure_free_pages(page_id.size_type());
      region->move_to_numa_node(page_id, _primary_buffer_pool->memory_node);
      _metrics->total_bytes_copied_from_ssd_to_dram.fetch_add(bytes_for_size_type(page_id.size_type()),
                                                              std::memory_order_relaxed);
    } else {
      // Case 4.2: We bypass load the page into NUMA
      _secondary_buffer_pool->ensure_free_pages(page_id.size_type());
      region->move_to_numa_node(page_id, _secondary_buffer_pool->memory_node);
      _metrics->total_bytes_copied_from_ssd_to_numa.fetch_add(bytes_for_size_type(page_id.size_type()),
                                                              std::memory_order_relaxed);
    }
    _ssd_region->read_page(page_id, region->get_page(page_id));
    return;
  }

  // Case 5: thepage should be one numa, check if we want to bypass
  DebugAssert(Frame::memory_node(state_before_exclusive) == _secondary_buffer_pool->memory_node, "Should be on NUMA");
  const auto bypass_dram =
      (access_intent == AccessIntent::Read && _config.migration_policy.bypass_dram_during_read()) ||
      (access_intent == AccessIntent::Write && _config.migration_policy.bypass_dram_during_write());

  if (bypass_dram) {
    // Case 5.1: Do nothing, stay on NUMA
    return;
  } else {
    // Case 5.2: Migrate to DRAM
    _secondary_buffer_pool->release_page(page_id.size_type());
    _primary_buffer_pool->ensure_free_pages(page_id.size_type());
    region->move_to_numa_node(page_id, _primary_buffer_pool->memory_node);
    _metrics->total_bytes_copied_from_numa_to_dram.fetch_add(bytes_for_size_type(page_id.size_type()),
                                                             std::memory_order_relaxed);
    return;
  }
}

void BufferManager::pin_for_read(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  _metrics->total_pins.fetch_add(1, std::memory_order_relaxed);
  _metrics->current_pins.fetch_add(1, std::memory_order_relaxed);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: use counter instead of while(true), make_resident?
  while (true) {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::LOCKED: {
        break;
      }
      case Frame::MARKED:
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, AccessIntent::Read, state_and_version);
          frame->unlock_exclusive();
        }
        break;
      }
      default: {
        // TODO: Still call make resident here? we actually have many reads now
        // and we should leverage the mechanism
        if (frame->try_lock_shared(state_and_version)) {
          return;
        }

        break;
      }
    }
    yield();
  }
}

void BufferManager::pin_for_write(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  _metrics->total_pins.fetch_add(1, std::memory_order_relaxed);
  _metrics->current_pins.fetch_add(1, std::memory_order_relaxed);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: use counter instead of while(true), make_resident?
  while (true) {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, AccessIntent::Write, state_and_version);
          return;
        }
        break;
      }
      case Frame::MARKED:
      case Frame::UNLOCKED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, AccessIntent::Write, state_and_version);
          return;
        }
        break;
      }
      default: {
        // The page is locked in shared or exclusive mode. Do nothing an wait.no5s
        yield();
      }
    }
  }
}

void BufferManager::unpin_for_read(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  _metrics->current_pins.fetch_sub(1, std::memory_order_relaxed);

  auto frame = get_region(page_id)->get_frame(page_id);
  if (frame->unlock_shared()) {
    add_to_eviction_queue(page_id, frame);
  }
}

void BufferManager::unpin_for_write(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  _metrics->current_pins.fetch_sub(1, std::memory_order_relaxed);

  auto frame = get_region(page_id)->get_frame(page_id);
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id, frame);
}

void BufferManager::set_dirty(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  get_region(page_id)->get_frame(page_id)->set_dirty(true);
}

void BufferManager::protect_page(PageID page_id) {
  if constexpr (ENABLE_MPROTECT) {
    auto data = get_region(page_id)->get_page(page_id);
    if (mprotect(data, bytes_for_size_type(page_id.size_type()), PROT_NONE) != 0) {
      const auto error = errno;
      Fail("Failed to mprotect: " + strerror(errno));
    }
  }
}

void BufferManager::unprotect_page(PageID page_id) {
  if constexpr (ENABLE_MPROTECT) {
    auto data = get_region(page_id)->get_page(page_id);
    if (mprotect(data, bytes_for_size_type(page_id.size_type()), PROT_READ | PROT_WRITE) != 0) {
      const auto error = errno;
      Fail("Failed to mprotect: " + strerror(errno));
    }
  }
}

size_t BufferManager::current_bytes_used_dram() const {
  return _primary_buffer_pool->used_bytes.load(std::memory_order_relaxed);
};

size_t BufferManager::current_bytes_used_numa() const {
  return _secondary_buffer_pool->used_bytes.load(std::memory_order_relaxed);
};

BufferManager::Config BufferManager::config() const {
  return _config;
}

StateVersionType BufferManager::_state(const PageID page_id) {
  const auto frame = _volatile_regions[static_cast<uint64_t>(page_id.size_type())]->get_frame(page_id);
  return Frame::state(frame->state_and_version());
}

PageID BufferManager::find_page(const void* ptr) const {
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _mapped_region;
  const auto region_idx = offset / DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_size =
      bytes_for_size_type(MIN_PAGE_SIZE_TYPE) * (1 << region_idx);  // TODO: this might break if not exponential sizes
  const auto region_offset = offset % DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_idx = region_offset / page_size;
  const auto valid = region_idx < NUM_PAGE_SIZE_TYPES && region_idx >= 0;
  const auto size_type = valid ? magic_enum::enum_value<PageSizeType>(region_idx) : MIN_PAGE_SIZE_TYPE;
  return PageID{size_type, static_cast<PageID::PageIDType>(page_idx), valid};
}

void BufferManager::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  if (frame->memory_node() == _primary_buffer_pool->memory_node) {
    _primary_buffer_pool->add_to_eviction_queue(page_id, frame);
  } else if (frame->memory_node() == _secondary_buffer_pool->memory_node) {
    _secondary_buffer_pool->add_to_eviction_queue(page_id, frame);
  } else {
    Fail("Cannot find buffer pool for given memory node");
  }
}

void* BufferManager::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto size_type = find_fitting_page_size_type(bytes);

  const auto [page_id, ptr] = _volatile_regions[static_cast<uint64_t>(size_type)]->allocate();

  _metrics->num_allocs.fetch_add(1, std::memory_order_relaxed);
  _metrics->total_allocated_bytes.fetch_add(bytes_for_size_type(size_type), std::memory_order_relaxed);

  return ptr;
}

void BufferManager::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  const auto page_id = find_page(p);
  auto region = get_region(page_id);
  // region->deallocate(page_id);
  // TODO Mark as dealloczed and iulock?
  add_to_eviction_queue(page_id, region->get_frame(page_id));
  // TODO: Properly handle deallocation, set to UNLOCKED inisitially
  _metrics->num_deallocs.fetch_add(1, std::memory_order_relaxed);
}

bool BufferManager::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

std::shared_ptr<VolatileRegion> BufferManager::get_region(const PageID page_id) {
  return _volatile_regions[static_cast<uint64_t>(page_id.size_type())];
}

std::shared_ptr<BufferManagerMetrics> BufferManager::metrics() {
  return _metrics;
}

size_t BufferManager::memory_consumption() const {
  // TODO: Fix number of frames
  //   const size_t volatile_regions_bytes = std::accumulate(_volatile_regions.begin(), _volatile_regions.end(),
  //                                                         [](auto region) { return region->memory_consumption(); });
  return 0;
}

//----------------------------------------------------
// Buffer Pool
//----------------------------------------------------

BufferManager::BufferPool::BufferPool(const size_t pool_size, const bool enable_eviction_purge_worker,
                                      std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> volatile_regions,
                                      MigrationPolicy migration_policy, std::shared_ptr<SSDRegion> ssd_region,
                                      std::shared_ptr<BufferPool> target_buffer_pool,
                                      std::shared_ptr<BufferManagerMetrics> metrics, const NumaMemoryNode memory_node)
    : max_bytes(pool_size),
      used_bytes(0),
      volatile_regions(volatile_regions),
      eviction_queue(std::make_unique<EvictionQueue>()),
      memory_node(memory_node),
      ssd_region(ssd_region),
      metrics(metrics),
      target_buffer_pool(target_buffer_pool),
      migration_policy(migration_policy),
      eviction_purge_worker(enable_eviction_purge_worker
                                ? std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE,
                                                                       [&](size_t) { this->purge_eviction_queue(); })
                                : nullptr) {}

void BufferManager::BufferPool::purge_eviction_queue() {
  auto item = EvictionItem{};
  for (auto i = size_t{0}; 0 < MAX_EVICTION_QUEUE_PURGES; ++i) {
    if (!eviction_queue->try_pop(item)) {
      return;
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // The item is in state UNLOCKED and can be marked
    if (item.can_mark(current_state_and_version)) {
      frame->try_mark(current_state_and_version);
      add_to_eviction_queue(item.page_id, frame);
      continue;
    }

    // The item is in state MARKED and can be evicted soon
    if (item.can_evict(current_state_and_version)) {
      eviction_queue->push(item);
      continue;
    }

    // The item is either LOCKED or the version is outdated,
    // we can just keep it removed
  }
}

void BufferManager::BufferPool::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  auto current_state_and_version = frame->state_and_version();
  DebugAssert(frame->memory_node() == memory_node, "Memory node mismatch");
  eviction_queue->push({page_id, Frame::version(current_state_and_version)});
}

void BufferManager::BufferPool::release_page(const PageSizeType size) {
  used_bytes.fetch_sub(bytes_for_size_type(size));
}

void BufferManager::BufferPool::ensure_free_pages(const PageSizeType required_size) {
  // TODO: Free at least 64 * PageSite bytes to reduce TLB shootdowns
  const auto bytes_required = bytes_for_size_type(required_size);
  auto freed_bytes = size_t{0};
  auto current_bytes = used_bytes.fetch_add(bytes_required);

  auto item = EvictionItem{};

  // Find potential victim frame if we don't have enough space left
  // TODO: Verify, that this is correct
  while ((int64_t)current_bytes + (int64_t)bytes_required - (int64_t)freed_bytes > (int64_t)max_bytes) {
    if (!eviction_queue->try_pop(item)) {
      Fail(
          "Cannot pop item from queue. All frames seems to be pinned. Please increase the memory size of this buffer "
          "pool.");
    }

    auto region = volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region->get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // TODO: Evict with wrogng target node

    // If the frane is UNLOCKED, we can mark it
    if (item.can_mark(current_state_and_version)) {
      frame->try_mark(current_state_and_version);
      add_to_eviction_queue(item.page_id, frame);
      continue;
    }

    // If the frame is already marked, we can evict it
    if (!item.can_evict(current_state_and_version)) {
      continue;
    }

    // Try locking the frame exclusively
    if (!frame->try_lock_exclusive(current_state_and_version)) {
      continue;
    }

    DebugAssert(frame->memory_node() == memory_node, "Memory node mismatch");

    const auto size_type = item.page_id.size_type();
    const auto num_bytes = bytes_for_size_type(size_type);

    // If we have a target buffer pool and we don't want to bypass it, we move the page to the other pool
    const auto write_to_ssd = !target_buffer_pool || migration_policy.bypass_numa_during_write();

    if (write_to_ssd) {
      // Otherwise we just write the page if its dirty and free the associated pages
      if (frame->is_dirty()) {
        auto data = region->get_page(item.page_id);
        ssd_region->write_page(item.page_id, data);  // TODO: use global function
        // TODO: protect_page(page_id);
        frame->reset_dirty();
      }
      region->free(item.page_id);
      frame->unlock_exclusive_and_set_evicted();
      // TODO: Improve if else, this does not work
      if (!target_buffer_pool) {
        metrics->total_bytes_copied_from_numa_to_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
      } else {
        metrics->total_bytes_copied_from_dram_to_ssd.fetch_add(num_bytes, std::memory_order_relaxed);
      }
    } else {
      // Or we just move to other numa node and unlock again
      target_buffer_pool->ensure_free_pages(size_type);
      region->move_to_numa_node(item.page_id, target_buffer_pool->memory_node);
      frame->unlock_exclusive();
      metrics->total_bytes_copied_from_dram_to_numa.fetch_add(num_bytes, std::memory_order_relaxed);
    }
    freed_bytes += num_bytes;
    current_bytes -= used_bytes.load();
  }

  used_bytes.fetch_sub(freed_bytes);
}

bool BufferManager::BufferPool::enabled() const {
  return memory_node != NO_NUMA_MEMORY_NODE;
}

}  // namespace hyrise