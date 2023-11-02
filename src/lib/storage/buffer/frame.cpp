#include "frame.hpp"
#include <bitset>

namespace hyrise {

Frame::Frame() {
  _state_and_version.store(update_state_with_same_version(0, EVICTED), std::memory_order_release);
}

void Frame::set_node_id(const NodeID node_id) {
  DebugAssert(node_id <= (NODE_ID_MASK >> NODE_ID_SHIFT), "Node ID must be smaller than 16.");
  DebugAssert(node_id != INVALID_NODE_ID, "Cannot set empty numa node");
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set memory node.");
  auto old_state_and_version = _state_and_version.load();
  while (true) {
    auto new_state_and_version =
        old_state_and_version ^
        ((old_state_and_version ^ static_cast<Frame::StateVersionType>(node_id) << NODE_ID_SHIFT) & NODE_ID_MASK);
    if (_state_and_version.compare_exchange_strong(old_state_and_version, new_state_and_version)) {
      DebugAssert((old_state_and_version & ~NODE_ID_MASK) == (new_state_and_version & ~NODE_ID_MASK),
                  "Settings the numa node failed");
      break;
    }
  }

  DebugAssert(Frame::node_id(_state_and_version.load()) == node_id,
              "Setting numa node didnt work: " + std::to_string(Frame::node_id(_state_and_version.load())));
}

void Frame::set_dirty(const bool new_dirty) {
  DebugAssert(state(_state_and_version.load()) == LOCKED, "Frame must be locked to set dirty flag.");
  Frame::StateVersionType dirty = new_dirty;
  _state_and_version.fetch_or(DIRTY_MASK & (dirty << DIRTY_SHIFT));
  DebugAssert(is_dirty() == new_dirty, "Setting dirty didnt work");
}

void Frame::reset_dirty() {
  _state_and_version &= ~DIRTY_MASK;
}

void Frame::unlock_exclusive_and_set_evicted() {
  Assert(state(_state_and_version.load()) == LOCKED, "Frame must be marked to set evicted flag.");
  _state_and_version.store(update_state_with_increment_version(_state_and_version.load(), EVICTED),
                           std::memory_order_release);
}

bool Frame::try_mark(Frame::StateVersionType old_state_and_version) {
  DebugAssert(state(_state_and_version.load()) == UNLOCKED,
              "Frame must be unlocked to mark, instead: " + std::to_string(state(_state_and_version.load())));
  return _state_and_version.compare_exchange_strong(old_state_and_version,
                                                    update_state_with_same_version(old_state_and_version, MARKED));
}

bool Frame::is_dirty() const {
  return (_state_and_version.load() & DIRTY_MASK) >> DIRTY_SHIFT;
}

Frame::StateVersionType Frame::state(Frame::StateVersionType state_and_version) {
  return (state_and_version & STATE_MASK) >> STATE_SHIFT;
}

Frame::StateVersionType Frame::version(Frame::StateVersionType state_and_version) {
  return state_and_version & VERSION_MASK;
}

Frame::StateVersionType Frame::state_and_version() const {
  return _state_and_version.load();
}

NodeID Frame::node_id() const {
  return node_id(_state_and_version.load());
}

NodeID Frame::node_id(Frame::StateVersionType state_and_version) {
  return static_cast<NodeID>((state_and_version & NODE_ID_MASK) >> NODE_ID_SHIFT);
}

bool Frame::try_lock_shared(Frame::StateVersionType old_state_and_version) {
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

bool Frame::try_lock_exclusive(Frame::StateVersionType old_state_and_version) {
  DebugAssert(state(old_state_and_version) == UNLOCKED || state(old_state_and_version) == MARKED ||
                  state(old_state_and_version) == EVICTED,
              "Frame must be unlocked, marked or evicted to lock exclusive, instead: " +
                  std::to_string(state(old_state_and_version)));
  return _state_and_version.compare_exchange_strong(old_state_and_version,
                                                    update_state_with_same_version(old_state_and_version, LOCKED));
}

bool Frame::unlock_shared() {
  while (true) {
    auto old_state_and_version = _state_and_version.load();
    auto old_state = state(old_state_and_version);
    Assert(old_state > 0 && old_state <= LOCKED_SHARED, "Frame must be locked shared to unlock shared.");
    auto new_state = old_state - 1;
    if (_state_and_version.compare_exchange_strong(old_state_and_version,
                                                   update_state_with_same_version(old_state_and_version, new_state))) {
      return new_state == Frame::UNLOCKED;  // return true if last shared lock was released
    }
  }
}

void Frame::unlock_exclusive() {
  Assert(state(_state_and_version.load()) == LOCKED,
         "Frame must be locked to unlock exclusive. " + std::to_string(state(_state_and_version.load())));
  _state_and_version.store(update_state_with_increment_version(_state_and_version.load(), UNLOCKED),
                           std::memory_order_release);
}

Frame::StateVersionType Frame::update_state_with_same_version(Frame::StateVersionType old_version_and_state,
                                                              Frame::StateVersionType new_state) {
  constexpr auto SHIFT = NUM_BITS - STATE_SHIFT;
  static_assert(SHIFT == 16, "Shift must be 8.");
  return ((old_version_and_state << SHIFT) >> SHIFT) | (new_state << STATE_SHIFT);
}

Frame::StateVersionType Frame::update_state_with_increment_version(Frame::StateVersionType old_version_and_state,
                                                                   Frame::StateVersionType new_state) {
  constexpr auto SHIFT = NUM_BITS - STATE_SHIFT;
  static_assert(SHIFT == 16, "Shift must be 8.");
  return (((old_version_and_state << SHIFT) >> SHIFT) + 1) | (new_state << STATE_SHIFT);
}

bool Frame::is_unlocked() const {
  return state(_state_and_version.load()) == UNLOCKED;
}

std::ostream& operator<<(std::ostream& os, const Frame& frame) {
  auto state = Frame::state(frame.state_and_version());
  auto version = Frame::version(frame.state_and_version());
  auto node_id = frame.node_id();
  auto dirty = frame.is_dirty();

  std::stringstream ss;
  ss << "Frame { state = ";

  switch (state) {
    case Frame::UNLOCKED:
      ss << "UNLOCKED";
      break;
    case Frame::LOCKED:
      ss << "LOCKED";
      break;
    case Frame::MARKED:
      ss << "MARKED";
      break;
    case Frame::EVICTED:
      ss << "EVICTED";
      break;
    default:
      ss << "LOCKED_SHARED (" << state << ")";
      break;
  }

  ss << ", node_id = " << node_id << ", dirty = " << dirty << ", version = " << version << "}";
  os << ss.str();
  return os;
}
}  // namespace hyrise
