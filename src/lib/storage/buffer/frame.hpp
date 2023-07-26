#pragma once

#include <atomic>
#include <bit>
#include "storage/buffer/types.hpp"

namespace hyrise {

// TODO: Encode New/ref counted page here, use 14 bits for it

class Frame {
 public:
  static constexpr StateVersionType UNLOCKED = 0;
  static constexpr StateVersionType LOCKED_SHARED = 0xFFFF - 3;  // 252 if 8 bits, 65532
  static constexpr StateVersionType LOCKED = 0xFFFF - 2;         // 253 if 8 bits, 65533
  static constexpr StateVersionType MARKED = 0xFFFF - 1;         // 254 if 8 bits, 65534
  static constexpr StateVersionType EVICTED = 0xFFFF;            // 255 if 8 bits, 65535 for 16 bits

  Frame();

  // Flags and metadata
  void set_numa_node(const NumaMemoryNode numa_node);
  void set_dirty(const bool new_dirty);
  bool is_dirty() const;
  void reset_dirty();
  NumaMemoryNode numa_node() const;

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
  static NumaMemoryNode numa_node(StateVersionType state_and_version);

  void debug_print();

 private:
  // clang-format off
  static constexpr uint64_t NUMA_NODE_MASK   = 0x00000F0000000000;
  static constexpr uint64_t DIRTY_MASK       = 0x0000F00000000000;
  static constexpr uint64_t STATE_MASK       = 0xFFFF000000000000;
  static constexpr uint64_t VERSION_MASK     = 0x000000FFFFFFFFFF;
  static_assert((NUMA_NODE_MASK ^ DIRTY_MASK ^ STATE_MASK ^ VERSION_MASK) == std::numeric_limits<StateVersionType>::max());
  // clang-format on

  static constexpr uint64_t NUM_BITS = sizeof(StateVersionType) * CHAR_BIT;
  static constexpr uint64_t NUMA_NODE_SHIFT = std::countr_zero(NUMA_NODE_MASK);
  static constexpr uint64_t DIRTY_SHIFT = std::countr_zero(DIRTY_MASK);
  static constexpr uint64_t STATE_SHIFT = std::countr_zero(STATE_MASK);

  StateVersionType update_state_with_same_version(StateVersionType old_version_and_state, StateVersionType new_state);
  StateVersionType update_state_with_increment_version(StateVersionType old_version_and_state,
                                                       StateVersionType new_state);

  std::atomic<StateVersionType> _state_and_version;
};

}  // namespace hyrise