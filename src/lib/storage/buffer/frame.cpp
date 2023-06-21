#include <bitset>
#include "storage/buffer/types.hpp"

namespace hyrise {

// TODO: Memory order

Frame::Frame() {
  _state_and_version.store(update_state_with_same_version(0, EVICTED));
}

void Frame::set_memory_node(const NumaMemoryNode memory_node) {
  DebugAssert(memory_node != NO_NUMA_MEMORY_NODE, "Cannot set empty numa node");
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set memory node.");
  _state_and_version |= (MEMORY_NODE_MASK & (static_cast<size_t>(memory_node) << MEMORY_NODE_SHIFT));
}

void Frame::set_dirty(const bool new_dirty) {
  // DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set dirty flag.");
  StateVersionType dirty = new_dirty;
  _state_and_version |= DIRTY_MASK & (dirty << DIRTY_SHIFT);
}

void Frame::reset_dirty() {
  _state_and_version &= ~DIRTY_MASK;
}

void Frame::unlock_exclusive_and_set_evicted() {
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be marked to set evicted flag.");
  _state_and_version.store(update_state_with_increment_version(_state_and_version.load(), EVICTED));
}

bool Frame::try_mark(StateVersionType old_state_and_version) {
  DebugAssert(state(_state_and_version.load()) == UNLOCKED,
              "Frame must be unlocked to mark, instead: " + std::to_string(state(_state_and_version.load())));
  return _state_and_version.compare_exchange_strong(old_state_and_version,
                                                    update_state_with_same_version(old_state_and_version, MARKED));
}

bool Frame::is_dirty() const {
  return (_state_and_version.load() & DIRTY_MASK) >> DIRTY_SHIFT;
}

StateVersionType Frame::state(StateVersionType state_and_version) {
  return (state_and_version & STATE_MASK) >> STATE_SHIFT;
}

StateVersionType Frame::version(StateVersionType state_and_version) {
  return state_and_version & VERSION_MASK;
}

StateVersionType Frame::state_and_version() const {
  return _state_and_version.load();
}

NumaMemoryNode Frame::memory_node() const {
  return memory_node(_state_and_version.load());
}

NumaMemoryNode Frame::memory_node(StateVersionType state_and_version) {
  return static_cast<NumaMemoryNode>((state_and_version & MEMORY_NODE_MASK) >> MEMORY_NODE_SHIFT);
}

bool Frame::try_lock_shared(StateVersionType old_state_and_version) {
  auto old_state = state(old_state_and_version);
  if (old_state < LOCKED_SHARED) {
    return _state_and_version.compare_exchange_strong(
        old_state_and_version, update_state_with_same_version(old_state_and_version, old_state + 1));
  }
  if (old_state == MARKED) {
    return _state_and_version.compare_exchange_strong(old_state_and_version,
                                                      update_state_with_same_version(old_state_and_version, 1));
  }
  return false;
}

bool Frame::try_lock_exclusive(StateVersionType old_state_and_version) {
  return _state_and_version.compare_exchange_strong(old_state_and_version,
                                                    update_state_with_same_version(old_state_and_version, LOCKED));
}

bool Frame::unlock_shared() {
  while (true) {
    auto old_state_and_version = _state_and_version.load();
    auto old_state = state(old_state_and_version);
    DebugAssert(old_state > 0 && old_state <= LOCKED_SHARED, "Frame must be locked shared to unlock shared.");
    auto new_state = old_state - 1;
    if (_state_and_version.compare_exchange_strong(old_state_and_version,
                                                   update_state_with_same_version(old_state_and_version, new_state))) {
      return new_state == Frame::UNLOCKED;  // return true if last shared lock was released
    }
  }
}

void Frame::unlock_exclusive() {
  DebugAssert(state(_state_and_version.load()) == LOCKED,
              "Frame must be locked to unlock exclusive. " + std::to_string(state(_state_and_version.load())));
  _state_and_version.store(update_state_with_increment_version(_state_and_version.load(), UNLOCKED),
                           std::memory_order_release);
}

StateVersionType Frame::update_state_with_same_version(StateVersionType old_version_and_state,
                                                       StateVersionType new_state) {
  constexpr auto SHIFT = NUM_BITS - STATE_SHIFT;
  static_assert(SHIFT == 8, "Shift must be 8.");
  return (old_version_and_state << SHIFT) >> SHIFT | (new_state << STATE_SHIFT);
}

StateVersionType Frame::update_state_with_increment_version(StateVersionType old_version_and_state,
                                                            StateVersionType new_state) {
  constexpr auto SHIFT = NUM_BITS - STATE_SHIFT;
  static_assert(SHIFT == 8, "Shift must be 8.");
  return ((old_version_and_state << SHIFT) >> SHIFT) + 1 | (new_state << STATE_SHIFT);
}

void Frame::debug_print() {
  std::cout << std::bitset<sizeof(_state_and_version) * CHAR_BIT>(_state_and_version.load()) << std::endl;
}

bool Frame::is_unlocked() const {
  return state(_state_and_version.load()) == UNLOCKED;
}

}  // namespace hyrise