#include "storage/buffer/types.hpp"
#include "storage/buffer/buffer_manager.hpp"

namespace hyrise {

bool EvictionItem::can_evict(StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::MARKED && Frame::version(state_and_version) == timestamp;
}

bool EvictionItem::can_mark(StateVersionType state_and_version) const {
  return Frame::state(state_and_version) == Frame::UNLOCKED && Frame::version(state_and_version) == timestamp;
}

boost::container::pmr::memory_resource* get_buffer_manager_memory_resource() {
  return &BufferManager::get();
}

}  // namespace hyrise