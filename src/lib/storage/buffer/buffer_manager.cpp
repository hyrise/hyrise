#include "buffer_manager.hpp"
#include <sys/mman.h>
#include <unistd.h>
#include <algorithm>
#include <chrono>
#include <fstream>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/persistence_manager.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

uint64_t get_default_pool_size() {
  const auto page_size = static_cast<int64_t>(sysconf(_SC_PAGESIZE));
  const auto num_pages = static_cast<int64_t>(sysconf(_SC_PHYS_PAGES));
  const auto total_bytes = num_pages * page_size;
  return total_bytes * 8 / 10;
}

//----------------------------------------------------
// BufferManager
//----------------------------------------------------

BufferManager::BufferManager(const uint64_t pool_size, const std::filesystem::path ssd_path, const NodeID node_id)
    : _mapped_region(VolatileRegion::create_mapped_region()),
      _volatile_regions(VolatileRegion::create_volatile_regions(_mapped_region)),
      _persistence_manager(std::make_shared<PersistenceManager>(ssd_path)),
      _buffer_pool(pool_size, node_id, _volatile_regions, _persistence_manager) {}

BufferManager::~BufferManager() {
  VolatileRegion::unmap_region(_mapped_region);
}

BufferManager& BufferManager::operator=(BufferManager&& other) noexcept {
  if (&other != this) {
    _volatile_regions = other._volatile_regions;
    _persistence_manager = other._persistence_manager;
    // std::swap(_buffer_pool, other._buffer_pool);
    std::swap(_mapped_region, other._mapped_region);
    // TODO: check again
  }
  return *this;
}

// TODO: This can take several templates to improve branching
void BufferManager::make_resident(const PageID page_id, const Frame::StateVersionType state_before_exclusive) {
  // Check if the page was freshly allocated by checking the version. In this case, we want to use either DRAM or NUMA
  const auto version = Frame::version(state_before_exclusive);
  const auto is_evicted = Frame::state(state_before_exclusive) == Frame::EVICTED;

  // Case 1: The page is already on DRAM. This is the easy case.
  if (!is_evicted && Frame::node_id(state_before_exclusive) == _buffer_pool.node_id()) {
    _total_hits.fetch_add(1, std::memory_order_relaxed);
    return;
  }

  // Case 2: The page is not on DRAM and we don't have it on another memory node, so we need to load it from SSD
  auto region = get_region(page_id);
  region->_unprotect_page(page_id);
  DebugAssert(Frame::node_id(state_before_exclusive) == _buffer_pool.node_id(), "Not on DRAM node");
  retry_with_backoff([&, page_id = page_id]() { return _buffer_pool.ensure_free_pages(page_id.num_bytes()); });
  _persistence_manager->read_page(page_id, region->get_page(page_id));
  _total_misses.fetch_add(1, std::memory_order_relaxed);

  // increment_metric_counter(_metrics->total_bytes_copied_from_ssd_to_dram, page_id.num_bytes());
  return;
}

void BufferManager::pin_shared(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

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
          make_resident(page_id, state_and_version);
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
  _total_pins.fetch_add(1, std::memory_order_relaxed);
}

void BufferManager::pin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  const auto frame = get_region(page_id)->get_frame(page_id);

  retry_with_backoff([&]() {
    auto state_and_version = frame->state_and_version();
    switch (Frame::state(state_and_version)) {
      case Frame::EVICTED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, state_and_version);
          return true;
        }
        return false;
      }
      case Frame::MARKED:
      case Frame::UNLOCKED: {
        if (frame->try_lock_exclusive(state_and_version)) {
          make_resident(page_id, state_and_version);
          return true;
        }
        return false;
      }
    }
    return false;
  });

  _total_pins.fetch_add(1, std::memory_order_relaxed);
}

void BufferManager::unpin_shared(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  // increment_metric_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  if (frame->unlock_shared()) {
    _buffer_pool.add_to_eviction_queue(page_id);
  }
}

void BufferManager::unpin_exclusive(const PageID page_id) {
  DebugAssert(page_id.valid(), "Invalid page id");

  // increment_metric_counter(_metrics->current_pins, -1);
  auto frame = get_region(page_id)->get_frame(page_id);
  frame->unlock_exclusive();
  _buffer_pool.add_to_eviction_queue(page_id);
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
  const auto region_idx = offset / VolatileRegion::DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_size =
      bytes_for_size_type(MIN_PAGE_SIZE_TYPE) * (1 << region_idx);  // TODO: this might break if not exponential sizes
  const auto region_offset = offset % VolatileRegion::DEFAULT_RESERVED_VIRTUAL_MEMORY_PER_REGION;
  const auto page_idx = region_offset / page_size;
  const auto valid = region_idx < NUM_PAGE_SIZE_TYPES && region_idx >= 0;
  const auto size_type = valid ? magic_enum::enum_value<PageSizeType>(region_idx) : MIN_PAGE_SIZE_TYPE;
  return PageID{size_type, static_cast<PageID::PageIDType>(page_idx), valid};
}

std::shared_ptr<VolatileRegion> BufferManager::get_region(const PageID page_id) {
  return _volatile_regions[static_cast<uint64_t>(page_id.size_type())];
}

size_t BufferManager::memory_consumption() const {
  const auto volatile_regions_bytes =
      std::accumulate(_volatile_regions.begin(), _volatile_regions.end(), 0,
                      [](auto acc, auto region) { return acc + region->memory_consumption(); });
  const auto persistence_manager_bytes = _persistence_manager->memory_consumption();
  const auto pool_bytes = _buffer_pool.memory_consumption();
  return sizeof(*this) + volatile_regions_bytes + persistence_manager_bytes + pool_bytes;
}

const BufferPool& BufferManager::buffer_pool() const {
  return _buffer_pool;
}

}  // namespace hyrise