#include "buffer_manager.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <utility>
#include "helper.hpp"
#include "hyrise.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

//----------------------------------------------------
// BufferManager
//----------------------------------------------------

BufferManager::BufferPools BufferManager::create_buffer_pools(
    const BufferManagerConfig config,
    const std::array<std::shared_ptr<VolatileRegion>, NUM_PAGE_SIZE_TYPES> _volatile_regions,
    const std::shared_ptr<SSDRegion> _ssd_region, const std::shared_ptr<BufferManagerMetrics> _metrics) {
  auto pools = BufferManager::BufferPools();
  // We need to create raw pointer here, because we want to keep the constructor private, but then we cannot use make_shared
  if (config.memory_node != INVALID_NODE_ID) {
    pools[1] =
        std::shared_ptr<BufferPool>(new BufferPool(config.numa_buffer_pool_size, _volatile_regions,
                                                   config.migration_policy, _ssd_region, nullptr, config.memory_node));
  }
  pools[0] =
      std::shared_ptr<BufferPool>(new BufferPool(config.dram_buffer_pool_size, _volatile_regions,
                                                 config.migration_policy, _ssd_region, pools[1], config.cpu_node));
  return pools;
}

BufferManager::BufferManager() : BufferManager(BufferManagerConfig::from_env_or_default(Hyrise::get().topology)) {}

BufferManager::BufferManager(const BufferManagerConfig config)
    : _mapped_region(BufferManager::create_mapped_region()),
      _volatile_regions(BufferManager::create_volatile_regions(config, _mapped_region, _metrics)),
      _ssd_region(std::make_shared<SSDRegion>(config.ssd_path)),
      _metrics(std::make_shared<BufferManagerMetrics>()),
      _buffer_pools(create_buffer_pools(config, _volatile_regions, _ssd_region, _metrics)) {
  Assert(config.cpu_node != config.memory_node, "CPU and memory node must be different");
}

BufferManager::~BufferManager() {
  unmap_region(_mapped_region);
}

BufferManager& BufferManager::operator=(BufferManager&& other) noexcept {
  if (&other != this) {
    _metrics = other._metrics;
    _volatile_regions = other._volatile_regions;
    _ssd_region = other._ssd_region;
    std::swap(_buffer_pools, other._buffer_pools);
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
                                  const Frame::StateVersionType state_before_exclusive) {
  // Check if the page was freshly allocated by checking the version. In this case, we want to use either DRAM or NUMA
  const auto version = Frame::version(state_before_exclusive);
  const auto is_evicted = Frame::state(state_before_exclusive) == Frame::EVICTED;

  // Case 1: The page is already on DRAM. This is the easy case.
  if (!is_evicted && Frame::node_id(state_before_exclusive) == _buffer_pools[0]->node_id) {
    _metrics->total_hits.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  // Case 2: The page is not on DRAM and we don't have it on another memory node, so we need to load it from SSD
  auto region = get_region(page_id);
  region->unprotect_page(page_id);
  DebugAssert(Frame::node_id(state_before_exclusive) == _buffer_pools[0]->node_id, "Not on DRAM node");
  retry_with_backoff([&]() { return _buffer_pools[0]->ensure_free_pages(page_id.size_type()); });
  _ssd_region->read_page(page_id, region->get_page(page_id));
  increment_metric_counter(_metrics->total_misses);
  increment_metric_counter(_metrics->total_bytes_copied_from_ssd_to_dram, page_id.num_bytes());
  return;
}

void BufferManager::pin_shared(const PageID page_id, const AccessIntent accessIntent) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_metric_counter(_metrics->total_pins);
  increment_metric_counter(_metrics->current_pins);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: Another, make_resident?
  retry_with_backoff([&]() {
    const auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::LOCKED: {
        return false;
      }
      // case Frame::MARKED: TODO
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, accessIntent, state_and_version);
          frame->unlock_exclusive();
        }
        return false;
      }
      default: {
        // TODO: Still call make resident here? we actually have many reads now
        // and we should leverage the mechanism, addtional reads would benefit
        if (frame->try_lock_shared(state_and_version)) {
          return true;
        }
        return false;
      }
    }
    return false;
  });
}

void BufferManager::pin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_metric_counter(_metrics->total_pins);
  increment_metric_counter(_metrics->current_pins);

  const auto frame = get_region(page_id)->get_frame(page_id);
  // TODO: Use another make_resident?
  retry_with_backoff([&]() {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, AccessIntent::Write, state_and_version);
          return true;
        }
        return false;
      }
      case Frame::MARKED:
      case Frame::UNLOCKED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, AccessIntent::Write, state_and_version);
          return true;
        }
        return false;
      }
    }
    return false;
  });
}

void BufferManager::unpin_shared(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_metric_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  if (frame->unlock_shared()) {
    add_to_eviction_queue(page_id, frame);
  }
}

void BufferManager::unpin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  increment_metric_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  frame->unlock_exclusive();
  add_to_eviction_queue(page_id, frame);
}

void BufferManager::set_dirty(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  get_region(page_id)->get_frame(page_id)->set_dirty(true);
}

Frame::StateVersionType BufferManager::_state(const PageID page_id) {
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
  if (frame->node_id() == _buffer_pools[0]->node_id) {
    _buffer_pools[0]->add_to_eviction_queue(page_id, frame);
  } else if (frame->node_id() == _buffer_pools[1]->node_id) {
    DebugAssert(_buffer_pools[1], "Pool has to be enabled");
    _buffer_pools[1]->add_to_eviction_queue(page_id, frame);
  } else {
    Fail("Cannot find buffer pool for given memory node " + std::to_string(frame->node_id()));
  }
}

void* BufferManager::do_allocate(std::size_t bytes, std::size_t alignment) {
  const auto size_type = find_fitting_page_size_type(bytes);

  auto region = _volatile_regions[static_cast<uint64_t>(size_type)];
  const auto [page_id, frame, ptr] = region->allocate();

  increment_metric_counter(_metrics->num_allocs);
  increment_metric_counter(_metrics->total_allocated_bytes, bytes_for_size_type(size_type));

  auto state_and_version = frame->state_and_version();
  if (!frame->try_lock_exclusive(state_and_version)) {
    Fail("Could not lock page for exclusive access. This should not happen during an allocation.");
  }

  region->unprotect_page(page_id);
  // Use either NUMA or DRAM for allocation
  retry_with_backoff([&, page_id = page_id, buffer_pool = buffer_pool]() {
    return buffer_pool->ensure_free_pages(page_id.size_type());
  });
  region->mbind_to_numa_node(page_id, buffer_pool->node_id);
  frame->set_dirty(true);
  frame->unlock_exclusive();
  buffer_pool->add_to_eviction_queue(page_id, frame);
  return ptr;
}

void BufferManager::do_deallocate(void* p, std::size_t bytes, std::size_t alignment) {
  // region->deallocate(page_id);
  // TODO Mark as dealloczed and iulock?
  const auto page_id = find_page(p);
  add_to_eviction_queue(page_id, get_region(page_id)->get_frame(page_id));
  // TODO: Properly handle deallocation, set to UNLOCKED inisitially
  increment_metric_counter(_metrics->num_deallocs);
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
  const auto volatile_regions_bytes =
      std::accumulate(_volatile_regions.begin(), _volatile_regions.end(), 0,
                      [](auto acc, auto region) { return acc + region->memory_consumption(); });
  const auto ssd_region_bytes = _ssd_region->memory_consumption();
  const auto pools_bytes = std::accumulate(_buffer_pools.begin(), _buffer_pools.end(), 0, [](auto acc, auto pool) {
    return acc + (pool ? pool->memory_consumption() : 0);
  });
  const auto metrics_bytes = sizeof(*_metrics);
  return sizeof(*this) + volatile_regions_bytes + ssd_region_bytes + pools_bytes + metrics_bytes;
}

const BufferManager::BufferPools BufferManager::buffer_pools() const {
  return _buffer_pool;
}

}  // namespace hyrise