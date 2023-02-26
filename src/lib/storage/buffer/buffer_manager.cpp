#include "buffer_manager.hpp"
#include <algorithm>
#include <boost/uuid/uuid.hpp>
#include <chrono>
#include <cstdlib>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "utils/assert.hpp"

namespace hyrise {

constexpr PageSizeType find_fitting_page_size_type(const std::size_t value) {
  if (value <= static_cast<std::size_t>(PageSizeType::KiB32)) {
    return PageSizeType::KiB32;
  } else if (value <= static_cast<std::size_t>(PageSizeType::KiB64)) {
    return PageSizeType::KiB64;
  } else if (value <= static_cast<std::size_t>(PageSizeType::KiB128)) {
    return PageSizeType::KiB128;
  } else if (value <= static_cast<std::size_t>(PageSizeType::KiB256)) {
    return PageSizeType::KiB256;
  }
  Fail("Cannot fit input value to a PageSizeType");
}

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

BufferManager::BufferManager() : _num_pages(0), _frames(get_volatile_capacity_from_env() / Page32KiB::size()) {
  _ssd_region = std::make_unique<SSDRegion>(get_ssd_region_file_from_env());
  _volatile_region = std::make_unique<VolatileRegion>(get_volatile_capacity_from_env());
  Assert(_frames.size() == _volatile_region->capacity(), "Frames size need to be equal to volatile region capacity");
  _replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

BufferManager::BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region)
    : _num_pages(0),
      _ssd_region(std::move(ssd_region)),
      _volatile_region(std::move(volatile_region)),
      _frames(_volatile_region->capacity()) {
  _replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

std::pair<FrameID, Frame*> BufferManager::allocate_frame() {
  auto [frame_id, allocated_page] = _volatile_region->allocate();
  if (frame_id == INVALID_PAGE_ID) {
    const auto victim_frame_id = _replacement_strategy->find_victim();
    Assert(victim_frame_id != INVALID_FRAME_ID, "Returned invalid frame id");
    auto victim_page = _volatile_region->get_page(victim_frame_id);
    auto& victim_frame = _frames[victim_frame_id];

    // TODO: Pinc count is wrong
    // DebugAssert(victim_frame.pin_count.load() == 0, "The victim frame cannot be unpinned");
    if (victim_frame.dirty) {
      write_page(victim_frame.page_id, *victim_page);
    }
    _page_table.erase(victim_frame.page_id);
    frame_id = victim_frame_id;
    allocated_page = victim_page;
  }

  _frames[frame_id].data = allocated_page;

  return std::make_pair(frame_id, &_frames[frame_id]);
}

Frame* BufferManager::find_in_page_table(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");
  std::thread::id this_id = std::this_thread::get_id();
  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    _metrics.page_table_hits++;
    auto [_, frame] = *frame_in_page_table_it;
    // _replacement_strategy->record_frame_access(frame_id);
    return frame;
  }
  _metrics.page_table_misses++;
  return nullptr;
}

void BufferManager::read_page(const PageID page_id, Page32KiB& destination) {
  _ssd_region->read_page(page_id, destination);
  _metrics.total_bytes_read += Page32KiB::size();
}

void BufferManager::write_page(const PageID page_id, Page32KiB& source) {
  _ssd_region->write_page(page_id, source);
  _metrics.total_bytes_written += Page32KiB::size();
}

Page32KiB* BufferManager::get_page(const PageID page_id) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  if (const auto frame = find_in_page_table(page_id)) {
    return frame->data;
  }

  auto [frame_id, allocated_frame] = allocate_frame();

  // Update the frame metadata and read the page data
  allocated_frame->page_id = page_id;
  allocated_frame->dirty = false;
  allocated_frame->pin_count.store(0);
  read_page(page_id, *allocated_frame->data);

  // Save the frame to the replacement strategy
  _replacement_strategy->record_frame_access(frame_id);

  // Update the page table and metadata
  _page_table[allocated_frame->page_id] = allocated_frame;

  return allocated_frame->data;
}

PageID BufferManager::new_page() {
  auto [frame_id, allocated_frame] = allocate_frame();

  // Update the frame metadata
  allocated_frame->page_id = _num_pages;
  allocated_frame->dirty = true;
  allocated_frame->pin_count.store(0);

  // Save the frame to the replacement strategy
  _replacement_strategy->record_frame_access(frame_id);

  // Update the page table and metadata
  _page_table[allocated_frame->page_id] = allocated_frame;

  _num_pages++;

  return allocated_frame->page_id;
}

void BufferManager::unpin_page(const PageID page_id, const bool dirty) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  frame->dirty = dirty;
  frame->pin_count--;

  if (frame->pin_count.load() == 0) {  // TODO: Check atomics
    // TODO: Maybe inefficient, restructure?
    auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame->data));
    _replacement_strategy->unpin(frame_id);
  }
}

void BufferManager::pin_page(const PageID page_id) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  frame->pin_count++;

  // TODO: Maybe inefficient, restructure?
  auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame->data));
  _replacement_strategy->pin(frame_id);
}

uint32_t BufferManager::get_pin_count(const PageID page_id) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return 0;
  }
  return frame->pin_count.load();
}

bool BufferManager::is_dirty(const PageID page_id) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return false;
  }
  return frame->dirty.load();
}

void BufferManager::flush_page(const PageID page_id) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  if (frame->dirty) {
    write_page(page_id, *frame->data);
    frame->dirty = false;
  }
};

void BufferManager::remove_page(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame->data));
  _volatile_region->deallocate(frame_id);

  if (frame->dirty) {
    write_page(page_id, *frame->data);
    frame->dirty = false;
  }
  // TODO: Remove from clock
  // TODO: Remove from disk
  _page_table.erase(page_id);
}

std::pair<PageID, std::ptrdiff_t> BufferManager::get_page_id_and_offset_from_ptr(const void* ptr) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);

  auto frame_id = _volatile_region->get_frame_id_from_ptr(ptr);
  if (frame_id == INVALID_FRAME_ID) {
    return std::make_pair(INVALID_PAGE_ID, 0);
  }
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - _frames[frame_id].data->data();
  return std::make_pair(_frames[frame_id].page_id, std::ptrdiff_t{offset});
};

BufferManagedPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);

  const auto page_size_type = find_fitting_page_size_type(bytes);
  Assert(page_size_type == PageSizeType::KiB32, "Cannot allocate " + std::to_string(bytes) +
                                                    " bytes with maximum page size of " +
                                                    std::to_string(static_cast<std::size_t>(PageSizeType::KiB32)));

  // Update metrics
  // _metrics.allocations_in_bytes.push_back(bytes);
  _metrics.current_bytes_used += bytes;
  _metrics.total_unused_bytes += static_cast<std::size_t>(page_size_type) - bytes;  // TODO: Aligment
  _metrics.max_bytes_used = std::max(_metrics.max_bytes_used, _metrics.current_bytes_used);
  _metrics.total_allocated_bytes += bytes;
  _metrics.num_allocs++;

  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto page_id = new_page();
  return BufferManagedPtr<void>(page_id, 0);  // TODO: Use easier constrcutor without offset, no! alignment
}

void BufferManager::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  std::lock_guard<std::mutex> lock(_page_table_mutex);

  _metrics.current_bytes_used -= bytes;
  // TODO: What happens to page on SSD
  remove_page(ptr.get_page_id());
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

BufferManager::Metrics& BufferManager::metrics() {
  return _metrics;
}

BufferManager& BufferManager::operator=(BufferManager&& other) {
  if (&other != this) {
    std::unique_lock this_lock(_page_table_mutex, std::defer_lock);
    std::unique_lock other_lock(other._page_table_mutex, std::defer_lock);
    std::lock(this_lock, other_lock);

    _num_pages = other._num_pages;
    _ssd_region = std::move(other._ssd_region);
    _volatile_region = std::move(other._volatile_region);
    _page_table = std::move(other._page_table);
    _frames = std::move(other._frames);
    _replacement_strategy = std::move(other._replacement_strategy);
    _metrics = other._metrics;
  }
  return *this;
}

}  // namespace hyrise