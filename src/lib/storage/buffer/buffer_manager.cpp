#include "buffer_manager.hpp"
#include <algorithm>
#include <chrono>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "storage/buffer/ssd_region.hpp"
#include "storage/buffer/volatile_region.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::size_t get_volatile_capacity_from_env() {
  if (const auto volatile_capacity = std::getenv("HYRISE_BUFFER_MANAGER_VOLATILE_CAPACITY")) {
    return boost::lexical_cast<size_t>(volatile_capacity);
  } else {
    Fail("HYRISE_BUFFER_MANAGER_VOLATILE_CAPACITY not found in environment");
  }
}

std::filesystem::path get_ssd_region_file_from_env() {
  if (const auto ssd_region_path = std::getenv("HYRISE_BUFFER_MANAGER_PATH")) {
    if (std::filesystem::is_block_file(ssd_region_path)) {
      return ssd_region_path;
    }
    const auto path = std::filesystem::path(ssd_region_path);
    const auto now = std::chrono::system_clock::now();
    const auto timestamp = std::chrono::duration_cast<std::chrono::seconds>(now.time_since_epoch()).count();
    const auto db_file = std::filesystem::path{"hyrise-buffer-pool-" + std::to_string(timestamp) + ".bin"};
    return path / db_file;
  } else {
    Fail("HYRISE_BUFFER_MANAGER_PATH not found in environment");
  }
}

size_t get_numa_node_from_env() {
#if HYRISE_NUMA_SUPPORT
  if (const auto numa_node = std::getenv("HYRISE_NUMA_NODE")) {
    return boost::lexical_cast<size_t>(numa_node);
  } else {
    Fail("HYRISE_NUMA_NODE not found in environment");
  }
#else
  return NO_NUMA_MEMORY_NODE;
#endif
}

std::array<VolatileRegion, NUM_PAGE_SIZE_TYPES> make_buffer_pools(const size_t num_bytes) {
  return {
      VolatileRegion(PageSizeType::KiB8, num_bytes),   VolatileRegion(PageSizeType::KiB16, num_bytes),
      VolatileRegion(PageSizeType::KiB32, num_bytes),  VolatileRegion(PageSizeType::KiB64, num_bytes),
      VolatileRegion(PageSizeType::KiB128, num_bytes),
  };
}

BufferManager::BufferManager(const size_t num_bytes, std::filesystem::path path, const size_t memory_numa_node)
    : _num_pages(0),
      _used_bytes(0),
      _total_bytes(num_bytes),
      _ssd_region(std::make_unique<SSDRegion>(path)),
      _buffer_pools(make_buffer_pools(num_bytes)) {}

BufferManager::BufferManager()
    : BufferManager(get_volatile_capacity_from_env(), get_ssd_region_file_from_env(), get_numa_node_from_env()) {}

Frame* BufferManager::allocate_frame(const PageSizeType size_type) {
  const auto bytes_required = bytes_for_size_type(size_type);
  auto freed_bytes = size_t{0};
  const auto current_bytes = _used_bytes.load();

  Frame* frame = nullptr;
  auto item = EvictionItem{};

  while ((current_bytes + bytes_required - freed_bytes) > _total_bytes) {
    if (!_eviction_queue.try_pop(item)) {
      Fail("Cannot pop item from queue");
    }

    // If the timestamp does not match, the frame is too old and we just check the next one
    if (item.timestamp < item.frame->eviction_timestamp.load() || item.frame->pin_count.load() > 0) {
      continue;
    }

    // Remove from page table
    {
      std::lock_guard<std::mutex> lock(_page_table_mutex);
      _page_table.erase(item.frame->page_id);
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
      _buffer_pools[static_cast<size_t>(frame_size_type)].free(item.frame);
    }
  }

  _used_bytes.fetch_add(bytes_required - freed_bytes);
  DebugAssert(_used_bytes.load() <= _total_bytes, "Used bytes exceeds total bytes");

  // If we did not find a frame of the correct size, we allocate a new one
  if (!frame) {
    frame = _buffer_pools[static_cast<size_t>(size_type)].allocate();
    DebugAssert(frame, "Could not allocate frame");
  }
  frame->eviction_timestamp.store(0);

  return frame;
}

Frame* BufferManager::find_in_page_table(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  std::lock_guard<std::mutex> lock(_page_table_mutex);

  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    _metrics.page_table_hits++;
    auto [_, frame] = *frame_in_page_table_it;
    return frame;
  }
  _metrics.page_table_misses++;
  return nullptr;
}

void BufferManager::read_page(Frame* frame) {
  _ssd_region->read_page(frame->page_id, frame->size_type, frame->data);
  _metrics.total_bytes_read += bytes_for_size_type(frame->size_type);
}

void BufferManager::write_page(const Frame* frame) {
  _ssd_region->write_page(frame->page_id, frame->size_type, frame->data);
  _metrics.total_bytes_written += bytes_for_size_type(frame->size_type);
}

Frame* BufferManager::get_frame(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  if (const auto frame = find_in_page_table(page_id)) {
    return frame;
  }

  const auto size_type = _ssd_region->get_size_type(page_id);
  if (!size_type) {
    Fail("Cannot find page id in page directory");
  }
  auto allocated_frame = allocate_frame(*size_type);

  // Update the frame metadata and read the page data
  allocated_frame->page_id = page_id;
  allocated_frame->dirty = false;
  allocated_frame->pin_count.store(0);
  read_page(allocated_frame);

  // Update the page table and metadata
  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table[allocated_frame->page_id] = allocated_frame;
  }
  return allocated_frame;
}

std::byte* BufferManager::get_page(const PageID page_id) {
  return get_frame(page_id)->data;
}

Frame* BufferManager::new_page(const PageSizeType size_type) {
  auto frame = allocate_frame(size_type);
  DebugAssert(frame != nullptr, "Frame is null");
  DebugAssert(frame->data != nullptr, "Frame data is null");

  // Update the frame metadata
  frame->page_id = _num_pages;
  frame->dirty = true;
  frame->pin_count.store(0);

  _ssd_region->register_page(frame->page_id, size_type);

  // Add the frame to the page table with lock guard in new scope
  {
    std::lock_guard<std::mutex> lock(_page_table_mutex);
    _page_table[frame->page_id] = frame;
  }

  // TODO: remove
  _eviction_queue.push({frame, 0});

  // TODO: Atomic
  _num_pages++;

  return frame;
}

void BufferManager::unpin_page(const PageID page_id, const bool dirty) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    // TODO This could happpen, if the pin guard still exists, but the page is removed
    Fail("Cannot unpin a page that is not in the page table. Check if the PinGuard is properly used.");
    return;
  }

  frame->dirty = frame->dirty.load() || dirty;
  frame->pin_count--;
  if (frame->pin_count.load() == 0) {
    const auto timestamp_before_eviction = frame->eviction_timestamp++;
    // Emplace timestamp and frame into eviction queue
    _eviction_queue.push({frame, timestamp_before_eviction + 1});
  }
}

void BufferManager::pin_page(const PageID page_id) {
  auto frame = get_frame(page_id);

  frame->pin_count++;

  // TODO: Maybe inefficient, restructure?
  // auto frame_id = _volatile_region->to_frame_id(reinterpret_cast<void*>(frame->data));
}

uint32_t BufferManager::get_pin_count(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return 0;
  }
  return frame->pin_count.load();
}

bool BufferManager::is_dirty(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return false;
  }
  return frame->dirty.load();
}

void BufferManager::remove_page(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  DebugAssert(!frame->pin_count, "Page cannot be removed while pinned. This can be used to debug resizes.");

  frame->dirty = false;
  frame->eviction_timestamp.store(0);
  frame->pin_count.store(0);

  _eviction_queue.push({frame, 0});
}

std::tuple<PageID, PageSizeType, std::ptrdiff_t> BufferManager::unswizzle(const void* ptr) {
  for (auto& buffer_pool : _buffer_pools) {
    if (const auto frame = buffer_pool.unswizzle(ptr)) {
      return std::make_tuple(frame->page_id, frame->size_type, reinterpret_cast<const std::byte*>(ptr) - frame->data);
    }
  }

  return std::make_tuple(INVALID_PAGE_ID, PageSizeType::KiB8, 0);
};

BufferManagedPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  // TODO: check Alignment

  const auto page_size_type = find_fitting_page_size_type(bytes);

  // Update metrics
  _metrics.current_bytes_used += bytes;
  _metrics.total_unused_bytes += bytes_for_size_type(page_size_type) - bytes;
  _metrics.max_bytes_used = std::max(_metrics.max_bytes_used, _metrics.current_bytes_used);
  _metrics.total_allocated_bytes += bytes;
  _metrics.num_allocs++;

  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto frame = new_page(page_size_type);

  // TODO: Pin the page
  // frame->pin_count++;

  return BufferManagedPtr<void>(frame->page_id, 0,
                                frame->size_type);  // TODO: Use easier constrcutor without offset, no! alignment
}

void BufferManager::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  _metrics.current_bytes_used -= bytes;
  _metrics.max_bytes_used = std::max(_metrics.max_bytes_used, _metrics.current_bytes_used);

  // TODO: What happens to page on SSD
  remove_page(ptr.get_page_id());
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

BufferManager::Metrics& BufferManager::metrics() {
  return _metrics;
}

void BufferManager::soft_reset() {
  _num_pages = 0;
  _page_table = {};
  // _buffer_pools = std::move(make_buffer_pools(0));  // TODO
  _ssd_region = std::make_unique<SSDRegion>(_ssd_region->get_file_name());
}

BufferManager& BufferManager::operator=(BufferManager&& other) {
  if (&other != this) {
    std::unique_lock this_lock(_page_table_mutex, std::defer_lock);
    std::unique_lock other_lock(other._page_table_mutex, std::defer_lock);
    std::lock(this_lock, other_lock);

    _num_pages = other._num_pages.load();
    _ssd_region = std::move(other._ssd_region);
    // _buffer_pools = std::move(other._buffer_pools); TODO
    _page_table = std::move(other._page_table);
    _metrics = other._metrics;
  }
  return *this;
}

}  // namespace hyrise