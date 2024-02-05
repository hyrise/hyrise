#include "frame.hpp"

#include <bitset>
#include <ostream>
#include <string>

#include "types.hpp"
#include "utils/assert.hpp"

namespace hyrise {

Frame::Frame() {
  // The frame should have an initial state of EVICTED and version 0.
  _state_and_version.store(_update_state_with_same_version(0, EVICTED), std::memory_order_release);
}

void Frame::set_node_id(const NodeID node_id) {
  DebugAssert(node_id <= (_node_id_mask >> _node_id_shift), "NUMA node must be smaller than 128.");
  DebugAssert(node_id != INVALID_NODE_ID, "Cannot set empty NUMA node");
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set NUMA node.");
  auto old_state_and_version = _state_and_version.load();

  // Execute a compare-and-swap loop to set the NUMA node.
  while (true) {
    auto new_state_and_version =
        old_state_and_version ^
        ((old_state_and_version ^ static_cast<Frame::StateVersionType>(node_id) << _node_id_shift) & _node_id_mask);
    if (_state_and_version.compare_exchange_strong(old_state_and_version, new_state_and_version)) {
      DebugAssert((old_state_and_version & ~_node_id_mask) == (new_state_and_version & ~_node_id_mask),
                  "Settings the NUMA node failed.");
      break;
    }
  }

  DebugAssert(Frame::node_id(_state_and_version.load()) == node_id,
              "Setting NUMA node didnt work: " + std::to_string(Frame::node_id(_state_and_version.load())));
}

void Frame::mark_dirty() {
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set dirty flag.");
  _state_and_version |= _dirty_mask;
}

void Frame::reset_dirty() {
  _state_and_version &= ~_dirty_mask;
}

void Frame::unlock_exclusive_and_set_evicted() {
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be marked to set evicted flag.");
  _state_and_version.store(_update_state_with_incremented_version(_state_and_version.load(), EVICTED),
                           std::memory_order_release);
}

bool Frame::try_mark(const Frame::StateVersionType old_state_and_version) {
  DebugAssert(
      state(_state_and_version.load()) == UNLOCKED,
      "Frame must be UNLOCKED to transition to MARKED, instead: " + std::to_string(state(_state_and_version.load())));
  auto state_and_version = old_state_and_version;
  return _state_and_version.compare_exchange_strong(state_and_version,
                                                    _update_state_with_same_version(old_state_and_version, MARKED));
}

bool Frame::is_dirty() const {
  return (_state_and_version.load() & _dirty_mask) >> _dirty_shift;
}

Frame::StateVersionType Frame::state(const Frame::StateVersionType state_and_version) {
  return (state_and_version & _state_mask) >> _state_shift;
}

Frame::StateVersionType Frame::version(const Frame::StateVersionType state_and_version) {
  return state_and_version & _version_mask;
}

Frame::StateVersionType Frame::state_and_version() const {
  return _state_and_version.load();
}

NodeID Frame::node_id() const {
  return node_id(_state_and_version.load());
}

NodeID Frame::node_id(const Frame::StateVersionType state_and_version) {
  return static_cast<NodeID>((state_and_version & _node_id_mask) >> _node_id_shift);
}

bool Frame::try_lock_shared(const Frame::StateVersionType old_state_and_version) {
  auto old_state = state(old_state_and_version);
  auto state_and_version = old_state_and_version;

  // Multiple threads can try to lock shared concurrently until the state reaches MAX_LOCKED_SHARED.
  if (old_state < MAX_LOCKED_SHARED) {
    // Increment the state by 1 to add a new concurrent reader
    return _state_and_version.compare_exchange_strong(
        state_and_version, _update_state_with_same_version(old_state_and_version, old_state + 1));
  }

  // If the state is MARKED, we just set the frame to 1 reader.
  if (old_state == MARKED) {
    return _state_and_version.compare_exchange_strong(
        state_and_version, _update_state_with_same_version(old_state_and_version, SINGLE_LOCKED_SHARED));
  }
  return false;
}

bool Frame::try_lock_exclusive(const Frame::StateVersionType old_state_and_version) {
  DebugAssert(state(old_state_and_version) == UNLOCKED || state(old_state_and_version) == MARKED ||
                  state(old_state_and_version) == EVICTED,
              "Frame must be unlocked, marked or evicted to lock exclusive, instead: " +
                  std::to_string(state(old_state_and_version)));
  auto state_and_version = old_state_and_version;
  return _state_and_version.compare_exchange_strong(state_and_version,
                                                    _update_state_with_same_version(old_state_and_version, LOCKED));
}

bool Frame::unlock_shared() {
  while (true) {
    auto old_state_and_version = _state_and_version.load();
    const auto old_state = state(old_state_and_version);
    DebugAssert(old_state > 0 && old_state <= MAX_LOCKED_SHARED, "Frame must be locked shared to unlock shared.");
    // Decrement the state by 1 to remove a concurrent reader, the version stays the same for shared unlocks.
    const auto new_state = old_state - 1;
    if (_state_and_version.compare_exchange_strong(old_state_and_version,
                                                   _update_state_with_same_version(old_state_and_version, new_state))) {
      // Return true if last shared latch has been released.
      return new_state == Frame::UNLOCKED;
    }
  }
}

void Frame::unlock_exclusive() {
  DebugAssert(state(_state_and_version.load()) == LOCKED,
              "Frame must be locked to unlock exclusive. " + std::to_string(state(_state_and_version.load())));
  _state_and_version.store(_update_state_with_incremented_version(_state_and_version.load(), UNLOCKED),
                           std::memory_order_release);
}

Frame::StateVersionType Frame::_update_state_with_same_version(const Frame::StateVersionType old_version_and_state,
                                                               const Frame::StateVersionType new_state) {
  constexpr auto SHIFT = _bit_width - _state_shift;
  return ((old_version_and_state << SHIFT) >> SHIFT) | (new_state << _state_shift);
}

Frame::StateVersionType Frame::_update_state_with_incremented_version(
    const Frame::StateVersionType old_version_and_state, const Frame::StateVersionType new_state) {
  constexpr auto SHIFT = _bit_width - _state_shift;
  return (((old_version_and_state << SHIFT) >> SHIFT) + 1) | (new_state << _state_shift);
}

bool Frame::is_unlocked() const {
  return state(_state_and_version.load()) == UNLOCKED;
}

std::ostream& operator<<(std::ostream& os, const Frame& frame) {
  const auto state_and_version = frame.state_and_version();
  const auto state = Frame::state(state_and_version);
  const auto version = Frame::version(state_and_version);
  const auto node_id = frame.node_id();
  const auto dirty = frame.is_dirty();

  os << "Frame(state = ";

  switch (state) {
    case Frame::UNLOCKED:
      os << "UNLOCKED";
      break;
    case Frame::LOCKED:
      os << "LOCKED";
      break;
    case Frame::MARKED:
      os << "MARKED";
      break;
    case Frame::EVICTED:
      os << "EVICTED";
      break;
    default:
      os << "LOCKED_SHARED (" << state << ")";
      break;
  }

  os << ", node_id = " << node_id << ", dirty = " << dirty << ", version = " << version << ")";

  return os;
}
}  // namespace hyrise
