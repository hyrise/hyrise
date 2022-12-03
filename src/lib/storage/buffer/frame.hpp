#pragma once
#include <atomic>
#include "storage/buffer/page.hpp"
#include "types.hpp"

namespace hyrise {

struct Frame {
  PageID PageID;
  Page data;
  std::atomic_bool dirty;
  std::atomic_uint32_t pin_count;
};

}  // namespace hyrise