#pragma once

#include <atomic>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

struct Frame {
  PageID page_id = INVALID_PAGE_ID;
  // TODO: Add its won frame id here
  std::atomic_bool dirty{false};
  std::atomic_uint32_t pin_count{0};
  Page<PageSizeType::KiB32>* data = nullptr;
};

}  // namespace hyrise