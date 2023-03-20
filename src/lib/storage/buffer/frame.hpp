#pragma once

#include <atomic>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

struct Frame {
  // Metadata used for identifcation of a buffer frame
  PageID page_id = INVALID_PAGE_ID;
  PageSizeType size_type;

  // Dirty and pin state
  std::atomic_bool dirty{false};
  std::atomic_uint32_t pin_count{0};

  // Used for eviction_queue
  std::atomic_uint64_t eviction_timestamp{0};

  // Pointer to raw data in volatile region
  std::byte* data;

  Frame* next_free_frame;
};

}  // namespace hyrise