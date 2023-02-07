#pragma once
#include <atomic>
#include "storage/buffer/page.hpp"
#include "storage/buffer/types.hpp"


namespace hyrise {

struct Frame {
  PageID page_id;
  std::atomic_bool dirty;
  std::atomic_uint32_t pin_count;
  Page *data;
};

}  // namespace hyrise