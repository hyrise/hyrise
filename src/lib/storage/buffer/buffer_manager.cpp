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

//----------------------------------------------------
// Helper Functions for Memory Mapping and Yielding
//----------------------------------------------------

std::byte* create_mapped_region() {
#ifdef __APPLE__
  const int flags = MAP_PRIVATE | MAP_ANON | MAP_NORESERVE;
#elif __linux__
  const int flags = MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE;
#endif
  const auto mapped_memory =
      static_cast<std::byte*>(mmap(NULL, DEFAULT_RESERVED_VIRTUAL_MEMORY, PROT_READ | PROT_WRITE, flags, -1, 0));
#ifdef __linux__
  madvise(_mapped_memory, num_bytes, MADV_DONTFORK);
#endif

  if (mapped_memory == MAP_FAILED) {
    const auto error = errno;
    Fail("Failed to map volatile pool region: " + strerror(errno));
  }

  return mapped_memory;
}

std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> create_volatile_regions(std::byte* mapped_region) {
  DebugAssert(mapped_region != nullptr, "Region not properly mapped");
  auto array = std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>{};

  // Ensure that every region has the same amount of virtual memory
  // Round to the next multiple of the largest page size
  for (auto i = size_t{0}; i < NUM_PAGE_SIZE_TYPES; i++) {
    array[i] = std::make_unique<VolatileRegion>(magic_enum::enum_value<PageSizeType>(i),
                                                mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * i,
                                                mapped_region + DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION * (i + 1));
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
  // TODO: Upgrade yield based on counter
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
    config.memory_node =
        static_cast<NumaMemoryNode>(json.value("memory_node", static_cast<int8_t>(config.memory_node)));
    config.mode = magic_enum::enum_cast<BufferManagerMode>(json.value("mode", magic_enum::enum_name(config.mode)))
                      .value_or(config.mode);
    return config;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_CONFIG_JSON_PATH not found in environment");
  }
}

BufferManager::BufferManager() : BufferManager(Config::from_env()) {}

BufferManager::BufferManager(const Config config)
    : _config(config),
      _mapped_region(create_mapped_region()),
      _metrics(std::make_shared<Metrics>()),
      _volatile_regions(create_volatile_regions(_mapped_region)),
      _ssd_region(config.ssd_path, 1 << 26),  // TODO: imprive init of pools here
      _primary_buffer_pool(config.dram_buffer_pool_size, config.enable_eviction_purge_worker, _volatile_regions,
                           &_ssd_region, config.memory_node == NO_NUMA_MEMORY_NODE ? nullptr : &_secondary_buffer_pool,
                           DEFAULT_DRAM_NUMA_NODE),
      _secondary_buffer_pool(config.dram_buffer_pool_size, config.enable_eviction_purge_worker, _volatile_regions,
                             &_ssd_region, nullptr, config.memory_node) {}

BufferManager::~BufferManager() {
  unmap_region(_mapped_region);
}

BufferManager& BufferManager::operator=(BufferManager&& other) noexcept {
  if (&other != this) {
    _metrics = other._metrics;
    _config = std::move(other._config);
    _volatile_regions = std::move(other._volatile_regions);
    _ssd_region = std::move(_ssd_region);
    _primary_buffer_pool = std::move(_primary_buffer_pool);
    _secondary_buffer_pool = std::move(_secondary_buffer_pool);
    std::swap(_mapped_region, other._mapped_region);
  }
  return *this;
}

BufferManager& BufferManager::get() {
  return Hyrise::get().buffer_manager;
}

void BufferManager::read_page(const PageID page_id) {
  auto ptr = get_region(page_id).get_page(page_id);
  _ssd_region.read_page(page_id, ptr);
}

void BufferManager::write_page(const PageID page_id) {
  auto ptr = get_region(page_id).get_page(page_id);
  _ssd_region.write_page(page_id, ptr);
}

void BufferManager::make_resident(const PageID page_id, const Frame* frame, const AccessIntent access_intent) {
  DebugAssert(Frame::state(frame->state_and_version()) == Frame::LOCKED, "Frame needs to be locked in exclusive mode");
  const auto is_on_dram_node = frame->memory_node() == DEFAULT_DRAM_NUMA_NODE;

  // Check if we want to bypass the DRAM node
  const auto bypass_dram =
      (access_intent == AccessIntent::Read && _config.migration_policy.bypass_dram_during_read()) ||
      (access_intent == AccessIntent::Write && _config.migration_policy.bypass_dram_during_write());

  // Case 2: Check if we have another memory node
  if (_config.memory_node == NO_NUMA_MEMORY_NODE) {
    return;
  }
  // TODO
  get_region(page_id).move_to_numa_node(page_id, NO_NUMA_MEMORY_NODE);

  _primary_buffer_pool.free_pages(page_id.size_type());
  read_page(page_id);
}

void BufferManager::pin_for_read(const PageID page_id) {
  const auto frame = get_region(page_id).get_frame(page_id);
  // TODO: use counter instead of while(true), make_resident?
  while (true) {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::LOCKED: {
        break;
      }
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, frame, AccessIntent::Read);
          frame->unlock_exclusive();
        }
        break;
      }
      default: {
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
  const auto frame = get_region(page_id).get_frame(page_id);
  // TODO: use counter instead of while(true), make_resident?
  while (true) {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, frame, AccessIntent::Write);
          return;
        }
        break;
      }
      case Frame::MARKED:
      case Frame::UNLOCKED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          return;
        }
        break;
      }
      default: {
        yield();
      }
    }
  }
}

void BufferManager::unpin_for_read(const PageID page_id) {
  auto frame = get_region(page_id).get_frame(page_id);
  frame->unlock_shared();
  if (frame->is_unlocked()) {
    add_to_eviction_queue(page_id, frame);
  }
}

void BufferManager::unpin_for_write(const PageID page_id) {
  auto frame = get_region(page_id).get_frame(page_id);
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id, frame);
}

void BufferManager::set_dirty(const PageID page_id) {
  get_region(page_id).get_frame(page_id)->set_dirty(true);
}

PageID BufferManager::find_page(const void* ptr) const {
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _mapped_region;
  const auto region_idx = offset / DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_size =
      bytes_for_size_type(MIN_PAGE_SIZE_TYPE) * (1 << region_idx);  // TODO: this might break if not exponential sizes
  const auto region_offset = offset % DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_idx = region_offset / page_size;
  const auto valid = region_idx < NUM_PAGE_SIZE_TYPES || region_idx >= 0;
  const auto size_type = magic_enum::enum_value<PageSizeType>(region_idx);
  return PageID{size_type, static_cast<PageID::PageIDType>(page_idx), valid};
}

void BufferManager::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  if (frame->memory_node() == _primary_buffer_pool.memory_node) {
    _primary_buffer_pool.add_to_eviction_queue(page_id, frame);
  } else if (frame->memory_node() == _secondary_buffer_pool.memory_node) {
    _secondary_buffer_pool.add_to_eviction_queue(page_id, frame);
  } else {
    Fail("Cannot find buffer pool for given memory node");
  }
}

void* BufferManager::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto size_type = find_fitting_page_size_type(bytes);
  // TODO: _migration_policy.bypass_dram_during_write());

  // TODO: Ensure free pages
  _primary_buffer_pool.free_pages(size_type);

  const auto memory_node = DEFAULT_DRAM_NUMA_NODE;
  const auto ptr = _volatile_regions[static_cast<uint64_t>(size_type)]->allocate(memory_node);
  // TODO: move toregion

  _metrics->num_allocs++;
  return ptr;
}

void BufferManager::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  const auto page_id = find_page(p);
  auto& region = get_region(page_id);
  // region.deallocate(page_id);
  // add_to_eviction_queue(page_id, region.get_frame(page_id));
  // TODO: Properly handle deallocation
  _metrics->num_deallocs++;
}

bool BufferManager::do_is_equal(const boost::container::pmr::memory_resource& other) const noexcept {
  return this == &other;
}

VolatileRegion& BufferManager::get_region(const PageID page_id) {
  return *_volatile_regions[static_cast<uint64_t>(page_id.size_type())];
}

std::shared_ptr<BufferManager::Metrics> BufferManager::metrics() {
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

BufferManager::BufferPool::BufferPool(
    const size_t pool_size, const bool enable_eviction_purge_worker,
    std::array<std::unique_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES>& volatile_regions, SSDRegion* ssd_region,
    BufferPool* target_buffer_pool, const NumaMemoryNode memory_node)
    : max_bytes(pool_size),
      used_bytes(0),
      volatile_regions(volatile_regions),
      eviction_queue(std::make_unique<EvictionQueue>()),
      memory_node(memory_node),
      ssd_region(ssd_region),
      target_buffer_pool(target_buffer_pool),
      eviction_purge_worker(enable_eviction_purge_worker
                                ? std::make_unique<PausableLoopThread>(IDLE_EVICTION_QUEUE_PURGE,
                                                                       [&](size_t) { this->purge_eviction_queue(); })
                                : nullptr) {}

void BufferManager::BufferPool::purge_eviction_queue() {
  Fail("TODO");
}

BufferManager::BufferPool& BufferManager::BufferPool::operator=(BufferManager::BufferPool&& other) noexcept {
  if (&other != this) {
    used_bytes = other.used_bytes.load();
    max_bytes = other.max_bytes;
    memory_node = other.memory_node;
    eviction_purge_worker = std::move(other.eviction_purge_worker);
    eviction_queue = std::move(other.eviction_queue);
    volatile_regions = std::move(other.volatile_regions);
    // ssd_region = std::move(other.ssd_region);
    // TODO: ow about pool
  }
  return *this;
}

void BufferManager::BufferPool::add_to_eviction_queue(const PageID page_id, Frame* frame) {
  auto current_state_and_version = frame->state_and_version();
  DebugAssert(frame->memory_node() == memory_node, "Memory node mismatch");
  frame->try_mark(current_state_and_version);
  eviction_queue->push({page_id, Frame::version(current_state_and_version)});
}

void BufferManager::BufferPool::free_pages(const PageSizeType size) {
  // TODO: Free at least 64 * PageSite bytes to reduce TLB shootdowns
  const auto bytes_required = bytes_for_size_type(size);
  auto freed_bytes = size_t{0};
  auto current_bytes = used_bytes.fetch_add(bytes_required);

  // Find potential victim frame if we don't have enough space left
  auto item = EvictionItem{};

  // TODO: Verify, that this is correct
  while ((int64_t)current_bytes + (int64_t)bytes_required - (int64_t)freed_bytes > (int64_t)max_bytes) {
    if (!eviction_queue->try_pop(item)) {
      Fail(
          "Cannot pop item from queue. All frames seems to be pinned. Please increase the memory size of this buffer "
          "pool.");
    }

    auto& region = *volatile_regions[static_cast<uint64_t>(item.page_id.size_type())];
    auto frame = region.get_frame(item.page_id);
    auto current_state_and_version = frame->state_and_version();

    // If the frame can be marked, this is hat we do
    if (item.can_mark(current_state_and_version)) {
      frame->try_mark(current_state_and_version);
      continue;
    }

    if (!item.can_evict(current_state_and_version)) {
      continue;
    }

    if (!frame->try_lock_exclusive(current_state_and_version)) {
      continue;
    }

    DebugAssert(frame->memory_node() == memory_node, "Memory node mismatch");

    const auto num_bytes = bytes_for_size_type(item.page_id.size_type());

    if (target_buffer_pool) {
      Fail("TODO");
      // TODO: Migration policy
      // Free into other pool??
      // Or we just move to other numa node and unlock again
      const auto target_memory_node = target_buffer_pool->memory_node;
      region.move_to_numa_node(item.page_id, target_memory_node);
      frame->unlock_exclusive();
    } else {
      // If we only have 2 tiers or if we are in the middle tier, we want to evict to disk
      if (frame->is_dirty()) {
        auto data = region.get_page(item.page_id);
        ssd_region->write_page(item.page_id, data);
        frame->reset_dirty();
      }
      region.free(item.page_id);
      frame->unlock_exclusive();  // TODO: MIght not be necessary
      frame->set_evicted();
    }
    freed_bytes += num_bytes;
    current_bytes -= used_bytes.load();
  }

  used_bytes.fetch_sub(freed_bytes);
}

}  // namespace hyrise