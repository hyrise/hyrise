#include "storage/buffer/types.hpp"

namespace hyrise {

bool EvictionItem::can_evict() const {
  if (frame->eviction_timestamp != timestamp) {
    return false;
  }
  return frame->can_evict();
}

}  // namespace hyrise