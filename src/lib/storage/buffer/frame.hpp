#pragma once

#include <atomic>
#include "storage/buffer/types.hpp"

namespace hyrise {

struct SharedFrame;

struct Frame {
  // Metadata used for identifcation of a buffer frame
  PageID page_id = INVALID_PAGE_ID;
  PageSizeType size_type;

  PageType page_type = PageType::Invalid;

  // Dirty and pin state
  std::atomic_bool dirty{false};
  std::atomic_uint32_t pin_count{0};

  // Used for eviction_queue
  std::atomic_uint64_t eviction_timestamp{0};

  // Pointer to raw data in volatile region
  std::byte* data;

  // Back pointer to shared frame
  SharedFrame* shared_frame;

  Frame* next_free_frame;

  void init(const PageID page_id) {
    this->page_id = page_id;
    dirty = false;
    pin_count.store(0);
    eviction_timestamp.store(0);
  }
};

/**
 * Meta frame that holds a pointer to either the DRAM or NUMA frame or to both. The idea is taken from the Spitfire paper.
*/
struct SharedFrame {
  // TODO: Implement latches for both frame types, Should we keep the SSD Latch in the SSD Region?

  Frame* dram_frame;
  Frame* numa_frame;

  SharedFrame(Frame* dram_frame, Frame* numa_frame) : dram_frame(dram_frame), numa_frame(numa_frame) {
    // TODO DebugAssert(numa_frame->page_type == PageType::Numa, "Invalid page type");
    //  TODO DebugAssert(dram_frame->page_type == PageType::Dram, "Invalid page type");

    dram_frame->shared_frame = this;
    numa_frame->shared_frame = this;
  }

  SharedFrame(Frame* frame)
      : dram_frame(frame->page_type == PageType::Dram ? frame : nullptr),
        numa_frame(frame->page_type == PageType::Numa ? frame : nullptr) {
    // TODO DebugAssert(frame->page_type != PageType::Invalid, "Invalid page type");
    frame->shared_frame = this;
  }

  void link_dram_frame(Frame* frame) {
    // TODO DebugAssert(frame->page_type == PageType::Dram, "Invalid page type");
    dram_frame = frame;
    frame->shared_frame = this;
  }
};

}  // namespace hyrise