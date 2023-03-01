#pragma once

#include <atomic>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

struct Frame {
  PageID page_id = INVALID_PAGE_ID;
  FrameID frame_id = INVALID_FRAME_ID;
  std::atomic_bool dirty{false};
  std::atomic_uint32_t pin_count{0};
  PageSizeType size_type;
  std::byte* data = nullptr;
};

}  // namespace hyrise