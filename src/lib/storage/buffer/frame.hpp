#pragma once

#include <atomic>
#include <bit>
#include "storage/buffer/types.hpp"

namespace hyrise {

// TODO: memory order

class Frame {
 public:
  static constexpr StateVersionType UNLOCKED = 0;
  static constexpr StateVersionType LOCKED_SHARED = 252;
  static constexpr StateVersionType LOCKED = 253;
  static constexpr StateVersionType MARKED = 254;
  static constexpr StateVersionType EVICTED = 255;

  Frame();

  // Flags and metadata
  void set_memory_node(const NumaMemoryNode memory_node);
  void set_dirty(const bool new_dirty);
  bool is_dirty() const;
  void reset_dirty();
  NumaMemoryNode memory_node() const;

  // State transitions
  void unlock_exclusive_and_set_evicted();

  bool try_mark(StateVersionType old_state_and_version);

  bool try_lock_shared(StateVersionType old_state_and_version);

  bool try_lock_exclusive(StateVersionType old_state_and_version);

  // Removes a shared locked and returns true if the frame is now unlocked
  bool unlock_shared();

  void unlock_exclusive();

  bool is_unlocked() const;

  // State and version helper
  StateVersionType state_and_version() const;
  static StateVersionType state(StateVersionType state_and_version);
  static StateVersionType version(StateVersionType state_and_version);
  static NumaMemoryNode memory_node(StateVersionType state_and_version);

  void debug_print();

 private:
  //clang-format off
  static constexpr uint64_t MEMORY_NODE_MASK = 0x000F000000000000;
  static constexpr uint64_t DIRTY_MASK = 0x00F0000000000000;
  static constexpr uint64_t STATE_MASK = 0xFF00000000000000;
  static constexpr uint64_t VERSION_MASK = 0x0000FFFFFFFFFFFF;
  //clang-format on

  static constexpr uint64_t NUM_BITS = sizeof(StateVersionType) * CHAR_BIT;
  static constexpr uint64_t MEMORY_NODE_SHIFT = std::countr_zero(MEMORY_NODE_MASK);
  static constexpr uint64_t DIRTY_SHIFT = std::countr_zero(DIRTY_MASK);
  static constexpr uint64_t STATE_SHIFT = std::countr_zero(STATE_MASK);

  StateVersionType update_state_with_same_version(StateVersionType old_version_and_state, StateVersionType new_state);
  StateVersionType update_state_with_increment_version(StateVersionType old_version_and_state,
                                                       StateVersionType new_state);

  std::atomic<StateVersionType> _state_and_version;
};

}  // namespace hyrise