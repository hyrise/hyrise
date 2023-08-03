#include "buffer_manager.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <fstream>
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
    Fail("Failed to map volatile pool region: " + strerror(error));
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
    Fail("Failed to unmap volatile pool region: " + strerror(error));
  }
}

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
        static_cast<NumaMemoryNode>(json.value("memory_node", static_cast<int64_t>(config.memory_node)));
    config.cpu_node = static_cast<NumaMemoryNode>(json.value("cpu_node", static_cast<int64_t>(config.cpu_node)));
    config.enable_numa = json.value("enable_numa", config.enable_numa);

    return config;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH not found in environment");
  }
}

nlohmann::json BufferManager::Config::to_json() const {
  auto json = nlohmann::json{};
  json["dram_buffer_pool_size"] = dram_buffer_pool_size;
  json["numa_buffer_pool_size"] = numa_buffer_pool_size;
  json["ssd_path"] = ssd_path;
  json["migration_policy"]["dram_read_ratio"] = migration_policy.get_dram_read_ratio();
  json["migration_policy"]["dram_write_ratio"] = migration_policy.get_dram_write_ratio();
  json["migration_policy"]["numa_read_ratio"] = migration_policy.get_numa_read_ratio();
  json["migration_policy"]["numa_write_ratio"] = migration_policy.get_numa_write_ratio();
  json["enable_eviction_purge_worker"] = enable_eviction_purge_worker;
  json["memory_node"] = static_cast<int64_t>(memory_node);
  return json;
}

//----------------------------------------------------
// BufferManager
//----------------------------------------------------

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

BufferManager::BufferManager(const Config config)
    : _config(config),
      _mapped_region(create_mapped_region()),
      _volatile_regions(create_volatile_regions(_mapped_region, _metrics)),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path, _metrics)),  // TODO: imprive init of pools here
      _metrics(std::make_shared<BufferManagerMetrics>()),
      _primary_buffer_pool(std::make_shared<BufferPool>(true, config.dram_buffer_pool_size,
                                                        config.enable_eviction_purge_worker, _volatile_regions,
                                                        config.migration_policy, _ssd_region, _secondary_buffer_pool,
                                                        config.cpu_node, _metrics->dram_buffer_pool_metrics)),
      _secondary_buffer_pool(std::make_shared<BufferPool>(
          config.enable_numa, config.numa_buffer_pool_size, config.enable_eviction_purge_worker, _volatile_regions,
          config.migration_policy, _ssd_region, nullptr, config.memory_node, _metrics->numa_buffer_pool_metrics)) {
  Assert(config.cpu_node != config.memory_node, "CPU and memory node must be different");
}

BufferManager::~BufferManager() {
  unmap_region(_mapped_region);
}

BufferManager& BufferManager::operator=(BufferManager&& other) noexcept {
  if (&other != this) {
    _config = std::move(other._config);
    _metrics = other._metrics;
    _volatile_regions = other._volatile_regions;
    _ssd_region = other._ssd_region;
    _primary_buffer_pool = other._primary_buffer_pool;
    _secondary_buffer_pool = other._secondary_buffer_pool;
    std::swap(_mapped_region, other._mapped_region);
    // TODO: check again
  }
  return *this;
}

BufferManager& BufferManager::get() {
  return Hyrise::get().buffer_manager;
}

// TODO: This can take several templates to improve branching
void BufferManager::make_resident(const PageID page_id, const AccessIntent access_intent,
                                  const StateVersionType state_before_exclusive) {
  // TODO: retake the desiscion here if something
  // TODO: What happens for the allocate case? Inpret allocate as a write regarding mig policy -> new method for pin
  // Check if the page was freshly allocated by checking the version. In this case, we want to use either DRAM or NUMA
  const auto version = Frame::version(state_before_exclusive);
  const auto is_evicted = Frame::state(state_before_exclusive) == Frame::EVICTED;

  // Case 1: The page is already on DRAM. This is the easy case.
  if (!is_evicted && Frame::numa_node(state_before_exclusive) == _primary_buffer_pool->numa_node) {
    _metrics->total_hits.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  auto region = get_region(page_id);

  if (!_secondary_buffer_pool->enabled) {
    // Case 3: The page is not on DRAM and we don't have it on another memory node, so we need to load it from SSD
    region->unprotect_page(page_id);
    DebugAssert(Frame::numa_node(state_before_exclusive) == _primary_buffer_pool->numa_node, "Not on DRAM node");
    for (auto repeat = size_t{0}; repeat < MAX_REPEAT_COUNT; ++repeat) {
      if (!_primary_buffer_pool->ensure_free_pages(page_id.size_type())) {
        yield(repeat);
        continue;
      }
      _ssd_region->read_page(page_id, region->get_page(page_id));
      increment_counter(_metrics->total_misses);
      increment_counter(_metrics->total_bytes_copied_from_ssd_to_dram, page_id.num_bytes());
      return;
    }
    Fail(
        "Could not allocate page on DRAM. Try increasing the buffer pool size.");  // TODO: missing quite some item in queue
  }

  // Case 4: The page is evicted anyways, decide if numa should be bypassed or not and load the page
  if (is_evicted) {
    region->unprotect_page(page_id);

    for (auto repeat = size_t{0}; repeat < MAX_REPEAT_COUNT; ++repeat) {
      const auto bypass_numa =
          !_secondary_buffer_pool->enabled ||
          (access_intent == AccessIntent::Read && _config.migration_policy.bypass_numa_during_read()) ||
          (access_intent == AccessIntent::Write && _config.migration_policy.bypass_numa_during_write());
      if (bypass_numa) {
        // Case 4.1: We bypass NUMA and load directly into DRAM
        if (!_primary_buffer_pool->ensure_free_pages(page_id.size_type())) {
          yield(repeat);
          continue;
        }
        region->mbind_to_numa_node(page_id, _primary_buffer_pool->numa_node);
        increment_counter(_metrics->total_bytes_copied_from_ssd_to_dram, page_id.num_bytes());
      } else {
        // Case 4.2: We bypass load the page into NUMA
        if (!_secondary_buffer_pool->ensure_free_pages(page_id.size_type())) {
          yield(repeat);
          continue;
        }
        region->mbind_to_numa_node(page_id, _secondary_buffer_pool->numa_node);
        increment_counter(_metrics->total_bytes_copied_from_ssd_to_numa, page_id.num_bytes());
      }
      _ssd_region->read_page(page_id, region->get_page(page_id));
      increment_counter(_metrics->total_misses);
      return;
    }
    Fail("Could not allocate page on DRAM or NUMA for an evicted page. Try increasing the buffer pool sizes.");
  }

  // Case 5: thepage should be one numa, check if we want to bypass
  DebugAssert(Frame::numa_node(state_before_exclusive) == _secondary_buffer_pool->numa_node, "Should be on NUMA");
  for (auto repeat = size_t{0}; repeat < MAX_REPEAT_COUNT; ++repeat) {
    const auto bypass_dram =
        (access_intent == AccessIntent::Read && _config.migration_policy.bypass_dram_during_read()) ||
        (access_intent == AccessIntent::Write && _config.migration_policy.bypass_dram_during_write());

    if (bypass_dram) {
      // Case 5.1: Do nothing, stay on NUMA
      increment_counter(_metrics->total_hits);
      return;
    } else {
      // Case 5.2: Migrate to DRAM
      if (!_primary_buffer_pool->ensure_free_pages(page_id.size_type())) {
        yield(repeat);
        continue;
      }
      _secondary_buffer_pool->free_bytes(bytes_for_size_type(page_id.size_type()));
      region->mbind_to_numa_node(page_id, _primary_buffer_pool->numa_node);
      increment_counter(_metrics->total_hits);
      increment_counter(_metrics->total_bytes_copied_from_numa_to_dram, page_id.num_bytes());
      return;
    }
  }
  Fail("Could not allocate page on DRAM. Try increasing the buffer pool size.");
}

void BufferManager::pin_shared(const PageID page_id, const AccessIntent accessIntent) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_counter(_metrics->total_pins);
  increment_counter(_metrics->current_pins);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: Another, make_resident?
  for (auto repeat = size_t{0};; ++repeat) {
    auto state_and_version = frame->state_and_version();

    switch (Frame::state(state_and_version)) {
      case Frame::LOCKED: {
        break;
      }
      // case Frame::MARKED: TODO
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, accessIntent, state_and_version);
          frame->unlock_exclusive();
        }
        break;
      }
      default: {
        // TODO: Still call make resident here? we actually have many reads now
        // and we should leverage the mechanism, addtional reads would benefit
        if (frame->try_lock_shared(state_and_version)) {
          return;
        }
        break;
      }
    }
    yield(repeat);
  }
  Fail("Could not pin page for read");
}

void BufferManager::pin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_counter(_metrics->total_pins);
  increment_counter(_metrics->current_pins);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: Use another make_resident?
  for (auto repeat = size_t{0};; ++repeat) {
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
    }
    yield(repeat);
  }
  Fail("Could not pin page for write");
}

void BufferManager::unpin_shared(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  if (frame->unlock_shared()) {
    add_to_eviction_queue(page_id, frame);
  }
}

void BufferManager::unpin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id, frame);
}

void BufferManager::set_dirty(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  get_region(page_id)->get_frame(page_id)->set_dirty(true);
}

size_t BufferManager::reserved_bytes_dram_buffer_pool() const {
  return _primary_buffer_pool->used_bytes.load(std::memory_order_relaxed);
};

size_t BufferManager::reserved_bytes_numa_buffer_pool() const {
  return _secondary_buffer_pool->used_bytes.load(std::memory_order_relaxed);
};

size_t BufferManager::free_bytes_dram_node() const {
  return _primary_buffer_pool->free_bytes_node();
};

size_t BufferManager::free_bytes_numa_node() const {
  return _secondary_buffer_pool->free_bytes_node();
};

size_t BufferManager::total_bytes_dram_node() const {
  return _primary_buffer_pool->total_bytes_node();
};

size_t BufferManager::total_bytes_numa_node() const {
  return _secondary_buffer_pool->total_bytes_node();
};

BufferManager::Config BufferManager::config() const {
  return _config;
}

StateVersionType BufferManager::_state(const PageID page_id) {
  const auto frame = _volatile_regions[static_cast<uint64_t>(page_id.size_type())]->get_frame(page_id);
  return Frame::state(frame->state_and_version());
}

PageID BufferManager::find_page(const void* ptr) const {
  const auto offset = std::ptrdiff_t{reinterpret_cast<const std::byte*>(ptr) - _mapped_region};
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
  if (frame->numa_node() == _primary_buffer_pool->numa_node) {
    _primary_buffer_pool->add_to_eviction_queue(page_id, frame);
  } else if (frame->numa_node() == _secondary_buffer_pool->numa_node) {
    DebugAssert(_secondary_buffer_pool->enabled, "Pool has to be enabled");
    _secondary_buffer_pool->add_to_eviction_queue(page_id, frame);
  } else {
    Fail("Cannot find buffer pool for given memory node " + std::to_string(frame->numa_node()));
  }
}

void* BufferManager::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto size_type = find_fitting_page_size_type(bytes);

  auto region = _volatile_regions[static_cast<uint64_t>(size_type)];
  const auto [page_id, frame, ptr] = region->allocate();

  increment_counter(_metrics->num_allocs);
  increment_counter(_metrics->total_allocated_bytes, bytes_for_size_type(size_type));

  auto state_and_version = frame->state_and_version();
  if (!frame->try_lock_exclusive(state_and_version)) {
    Fail("Could not lock page for exclusive access. This should not happen during an allocation.");
  }

  region->unprotect_page(page_id);

  for (auto repeat = size_t{0}; repeat < MAX_REPEAT_COUNT; ++repeat) {
    // Use either NUMA or DRAM for allocation
    auto buffer_pool = _secondary_buffer_pool->enabled && !_config.migration_policy.bypass_numa_during_write()
                           ? _secondary_buffer_pool
                           : _primary_buffer_pool;
    if (!buffer_pool->ensure_free_pages(page_id.size_type())) {
      yield(repeat);
      continue;
    }
    region->mbind_to_numa_node(page_id, buffer_pool->numa_node);
    frame->set_dirty(true);
    frame->unlock_exclusive();
    buffer_pool->add_to_eviction_queue(page_id, frame);
    return ptr;
  }
  Fail("Could not allocate page on NUMA or DRAM. Try increasing both buffer pool sizes.");
}

void BufferManager::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  // region->deallocate(page_id);
  // TODO Mark as dealloczed and iulock?
  const auto page_id = find_page(p);
  add_to_eviction_queue(page_id, get_region(page_id)->get_frame(page_id));
  // TODO: Properly handle deallocation, set to UNLOCKED inisitially
  increment_counter(_metrics->num_deallocs);
}

bool BufferManager::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

std::shared_ptr<VolatileRegion> BufferManager::get_region(const PageID page_id) {
  return _volatile_regions[static_cast<uint64_t>(page_id.size_type())];
}

std::byte* BufferManager::_get_page_ptr(const PageID page_id) {
  return get_region(page_id)->get_page(page_id);
}

std::shared_ptr<BufferManagerMetrics> BufferManager::metrics() {
  return _metrics;
}

size_t BufferManager::memory_consumption() const {
  const auto volatile_regions_bytes =
      std::accumulate(_volatile_regions.begin(), _volatile_regions.end(), 0,
                      [](auto acc, auto region) { return acc + region->memory_consumption(); });
  const auto ssd_region_bytes = _ssd_region->memory_consumption();
  const auto buffer_pool_bytes = _primary_buffer_pool->memory_consumption();
  const auto secondary_buffer_pool_bytes = _secondary_buffer_pool->memory_consumption();
  const auto metrics_bytes = sizeof(*_metrics);
  return sizeof(*this) + volatile_regions_bytes + ssd_region_bytes + buffer_pool_bytes + secondary_buffer_pool_bytes +
         metrics_bytes;
}

size_t BufferManager::pool_size() const {
  return _primary_buffer_pool->max_bytes + (_secondary_buffer_pool->enabled ? _secondary_buffer_pool->max_bytes : 0);
}

}  // namespace hyrise