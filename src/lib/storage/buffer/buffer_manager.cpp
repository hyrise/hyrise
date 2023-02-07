#include "buffer_manager.hpp"
#include <cstdlib>
#include <utility>
#include "hyrise.hpp"
#include "storage/buffer/buffer_managed_ptr.hpp"
#include "utils/assert.hpp"

namespace hyrise {

BufferManager::BufferManager() : _num_pages(0) {
  if (const auto ssd_region_path = std::getenv("HYRISE_BUFFER_MANAGER_PATH")) {
    _ssd_region = std::make_unique<SSDRegion>(ssd_region_path);
  } else {
    Fail("HYRISE_BUFFER_MANAGER_PATH not found in environment");
  }
  if (const auto volatile_capacity = std::getenv("HYRISE_BUFFER_MANAGER_VOLATILE_CAPACITY")) {
    _volatile_region = std::make_unique<VolatileRegion>(boost::lexical_cast<size_t>(volatile_capacity));
  } else {
    Fail("HYRISE_BUFFER_MANAGER_VOLATILE_CAPACITY not found in environment");
  }
  _metrics.num_frames = _volatile_region->capacity();
  _frames.resize(_volatile_region->capacity());
  _replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

BufferManager::BufferManager(std::unique_ptr<VolatileRegion> volatile_region, std::unique_ptr<SSDRegion> ssd_region)
    : _num_pages(0), _ssd_region(std::move(ssd_region)), _volatile_region(std::move(volatile_region)) {
  _metrics.num_frames = volatile_region->capacity();
  _frames.resize(_volatile_region->capacity());
  _replacement_strategy = std::make_unique<ClockReplacementStrategy>(_volatile_region->capacity());
}

Frame* BufferManager::allocate_frame() {
  auto [frame_id, allocated_page] = _volatile_region->allocate();
  if (frame_id == INVALID_PAGE_ID) {
    const auto victim_frame_id = _replacement_strategy->find_victim();
    Assert(victim_frame_id != INVALID_FRAME_ID, "Returned invalid frame id");
    auto victim_page = _volatile_region->get_page(victim_frame_id);
    DebugAssert(victim_frame->pin_count == 0, "The victim frame cannot be unpinned");

    if (_frames[victim_frame_id].dirty) {
      write_page(_frames[victim_frame_id].page_id, *victim_page->data);
    }
    frame_id = victim_frame_id;
    allocated_page = victim_page;
  }

  auto frame = &_frames[frame_id];
  frame->page = allocated_page;

  return frame;
}

Frame* BufferManager::find_in_page_table(const PageID page_id) {
  DebugAssert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame_in_page_table_it = _page_table.find(page_id);
  if (frame_in_page_table_it != _page_table.end()) {
    _metrics.page_table_hits++;
    return frame;
  }
  _metrics.page_table_misses++;
  return nullptr;
}

PageID BufferManager::read_page(const PageID page_id, Page& destination) {
  _ssd_region->read_page(page_id, destination);
  _metrics.bytes_read += PAGE_SIZE;
}

PageID BufferManager::write_page(const PageID page_id, Page& source) {
 _ssd_region->write_page(page_id, destination);
 _metrics.bytes_written += PAGE_SIZE;
}


Page* BufferManager::get_page(const PageID page_id) {
  if (const auto frame = find_in_page_table(page_id)) {
    return frame->data;
  }

  auto allocated_frame = allocate_frame();

  allocated_frame->pin_count = 0;
  allocated_frame->page_id = page_id;
  allocated_frame->dirty = false;

  // TODO: Clock

  read_page(page_id, *allocated_frame->data);

  return allocated_frame->data;
}

PageID BufferManager::new_page() {
  auto allocated_frame = allocate_frame();

  allocated_frame->page_id = _num_pages;
  allocated_frame->dirty = false;
  allocated_frame->pin_count = 1;

  // TODO: Clock

  _page_table[allocated_frame->page_id] = allocated_frame;
  _num_pages++;

  return allocated_frame->page_id;
}

void BufferManager::unpin_page(const PageID page_id) {
  Assert(page_id != INVALID_PAGE_ID, "Page ID is invalid");

  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  frame->pin_count--;

  if (frame->pin_count == 0) {
    // TODO: Maybe inefficient, restructure?
    auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame));
    _replacement_strategy->unpin(frame_id);
  }
}

void BufferManager::pin_page(const PageID page_id) {
  const auto frame = find_in_page_table(page_id);
  if (frame == nullptr) {
    return;
  }

  frame->pin_count++;

  // TODO: Maybe inefficient, restructure?
  auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame));
  _replacement_strategy->pin(frame_id);
}

void BufferManager::flush_page(const PageID page_id) {
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

  auto frame_id = _volatile_region->get_frame_id_from_ptr(reinterpret_cast<void*>(frame));
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
  auto frame_id = _volatile_region->get_frame_id_from_ptr(ptr);
  auto frame = _frames[frame_id];
  const auto offset = reinterpret_cast<const std::byte*>(ptr) - frame->data.data.data();
  return std::make_pair(frame->page_id, std::ptrdiff_t{offset});
};

BufferManagedPtr<void> BufferManager::allocate(std::size_t bytes, std::size_t align) {
  Assert(bytes <= PAGE_SIZE, "Cannot allocate more than a Page currently");
  // TODO: Do Alignment with aligner, https://www.boost.org/doc/libs/1_62_0/doc/html/align.html
  const auto page_id = new_page();
  metrics.allocations_in_bytes.push_back(bytes);
  return BufferManagedPtr<void>(page_id, 0);  // TODO: Use easier constrcutor without offset, no! alignment
}

void BufferManager::deallocate(BufferManagedPtr<void> ptr, std::size_t bytes, std::size_t align) {
  remove_page(ptr.get_page_id());
}

BufferManager& BufferManager::get_global_buffer_manager() {
  return Hyrise::get().buffer_manager;
}

}  // namespace hyrise