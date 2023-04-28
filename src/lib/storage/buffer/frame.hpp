#pragma once

#include <atomic>
#include <mutex>
#include "storage/buffer/types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

class SharedFrame;

class Frame {
 public:
  enum class State { Evicted, Resident };

  const PageSizeType size_type;
  const PageType page_type;
  const PageID page_id;

  explicit Frame(const PageID page_id, const PageSizeType size_type, const PageType page_type,
                 std::byte* data = nullptr)
      : page_id(page_id), size_type(size_type), page_type(page_type), data(data) {}

  // State variables
  std::atomic_uint32_t pin_count{0};
  std::atomic<State> state;
  std::atomic_bool dirty{false};

  // Reference bit that is used for second chance eviction
  std::atomic_bool referenced{false};

  // Used for eviction_queue
  std::atomic_uint64_t eviction_timestamp{0};

  // Pointer to raw data in volatile region
  std::byte* data;

  std::mutex latch;

  std::weak_ptr<SharedFrame> shared_frame;

  // TODO: Store actual used size to reduce copy overhead?

  // Various helper functions
  bool can_evict() const;
  void set_evicted();
  void try_set_dirty(const bool new_dirty);
  bool is_resident() const;
  void set_resident();
  bool is_pinned() const;
  void clear();
  /**
   Check if the frame is second change evicable (reference bit is false). If the reference bit is true, it is set to false
  */
  bool try_second_chance_evictable();
  void set_referenced();
};

class SharedFrame {
 public:
  std::shared_ptr<Frame> dram_frame;
  std::shared_ptr<Frame> numa_frame;

  SharedFrame(std::shared_ptr<Frame> frame)
      : dram_frame(frame->page_type == PageType::Dram ? frame : nullptr),
        numa_frame(frame->page_type == PageType::Numa ? frame : nullptr) {
    DebugAssert(frame->page_type != PageType::Invalid, "Invalid page type");
  }

  static void link(const std::shared_ptr<SharedFrame>& shared_frame, const std::shared_ptr<Frame>& frame) {
    if (frame->page_type == PageType::Dram) {
      shared_frame->dram_frame = frame;
    } else if (frame->page_type == PageType::Numa) {
      shared_frame->numa_frame = frame;
    } else {
      Fail("Invalid page type");
    }
    frame->shared_frame = shared_frame;
  }

  // TODO: Destructor ensure evict
};

}  // namespace hyrise