#pragma once

#include <bit>

#include <atomic>

#include "types.hpp"

namespace hyrise {

/**
 * Frames are the metadata objects for each page. We are using a 64 bit atomic integer to store the latching state, NUMA node, dirty flag and the version of the frame.
 * All operations are atomic. The basic idea and most of the code is based on the SIGMOD'23 paper "Virtual-Memory Assisted Buffer Management" by Leis et. al.
 * 
 * NodeID and the dirty flag can be set independently from the state of the frame. The state is encoded in the upper 16 bits of the 64 bit integer. 
 * The state is encodoed as EVICTED, LOCKED, UNLOCKED, MARKED and LOCK_SHARED. Initially, the frame is in state EVICTED. Then, the page gets LOCKED for reading from disk.
 * After that, the page is UNLOCKED and can be used. It can be LOCKED again for writing. Or it can be changed into LOCK_SHARED for reading. LOCKED_SHARED can be executed
 * multiple times in parallel. The state MARKED is used for second-chance marking a page for eviction. The state is changed to EVICTED after the page is written to disk.
 * After unlocking a page, the version counter is updated. This is used to detect concurrent changes to the state of the frame. The reference implementation can be found
 * in https://github.com/viktorleis/vmcache/blob/master/vmcache.cpp.
 * 
 * 
 *                                                            try_lock_exclusive
 *                                                   +----------------------------------------------------------------------+
 *                                                   v                                                                      |
 * +---------+  try_lock_exclusive                 +--------+  unlock_exclusive     +----------------+  try_mark          +---------------------+
 * | EVICTED | ----------------------------------> |        | --------------------> |                | -----------------> |       MARKED        |
 * +---------+                                     |        |                       |                |                    +---------------------+
 *   ^         unlock_exclusive_and_set_evicted    |        |  try_lock_exclusive   |                |                        try_lock_shared
 *   +-------------------------------------------- | LOCKED | <-------------------- |    UNLOCKED    |                      +-----------------+
 *                                                 |        |                       |                |                      v                 |
 *                                                 |        |                       |                |  try_lock_shared   +---------------------+
 *                                                 |        |                       |                | -----------------> |    LOCKED_SHARED    | -+
 *                                                 +--------+                       +----------------+                    +---------------------+  |
 *                                                                                    ^                                     ^ unlock_shared   |    |
 *                                                                                    | unlock_shared                       +-----------------+    |
 *                                                                                    |                                                            |
 *                                                                                    |                                                            |
 *                                                                                    +------------------------------------------------------------+
 * Graphic was created with https://github.com/ggerganov/dot-to-ascii
*/

class Frame final : private Noncopyable {
 public:
  using StateVersionType = uint64_t;

  // State constants
  static constexpr StateVersionType UNLOCKED = 0;
  static constexpr StateVersionType MAX_LOCKED_SHARED = 0xFFFF - 3;  // 65532
  static constexpr StateVersionType LOCKED = 0xFFFF - 2;             // 65533
  static constexpr StateVersionType MARKED = 0xFFFF - 1;             // 65534
  static constexpr StateVersionType EVICTED = 0xFFFF;                // 65535

  Frame();

  // Flags and other metadata
  void set_node_id(const NodeID node_id);
  void mark_dirty();
  bool is_dirty() const;
  void reset_dirty();
  NodeID node_id() const;

  // State transitions
  void unlock_exclusive_and_set_evicted();

  bool try_mark(const StateVersionType old_state_and_version);

  bool try_lock_shared(const StateVersionType old_state_and_version);

  bool try_lock_exclusive(const StateVersionType old_state_and_version);

  // Removes a shared locked and returns true if the frame is now unlocked
  bool unlock_shared();

  void unlock_exclusive();

  bool is_unlocked() const;

  // State and version helper
  StateVersionType state_and_version() const;
  static StateVersionType state(const StateVersionType state_and_version);
  static StateVersionType version(const StateVersionType state_and_version);
  static NodeID node_id(const StateVersionType state_and_version);

 private:
  //TODO: Explan bit usage
  // clang-format off
  static constexpr uint64_t _NODE_ID_MASK     = 0x00000F0000000000;
  static constexpr uint64_t _DIRTY_MASK       = 0x0000F00000000000;
  static constexpr uint64_t _STATE_MASK       = 0xFFFF000000000000;
  static constexpr uint64_t _VERSION_MASK     = 0x000000FFFFFFFFFF;
  // clang-format on

  static_assert((_NODE_ID_MASK ^ _DIRTY_MASK ^ _STATE_MASK ^ _VERSION_MASK) ==
                    std::numeric_limits<StateVersionType>::max(),
                "The given masks either overlap or do not cover the whole StateVersionType.");

  static constexpr uint64_t _BIT_WIDTH = sizeof(StateVersionType) * std::numeric_limits<unsigned char>::digits;
  static constexpr uint64_t _NODE_ID_SHIFT = std::countr_zero(_NODE_ID_MASK);
  static constexpr uint64_t _DIRTY_SHIFT = std::countr_zero(_DIRTY_MASK);
  static constexpr uint64_t _STATE_SHIFT = std::countr_zero(_STATE_MASK);

  StateVersionType _update_state_with_same_version(const StateVersionType old_version_and_state,
                                                   const StateVersionType new_state);
  StateVersionType _update_state_with_increment_version(const StateVersionType old_version_and_state,
                                                        const StateVersionType new_state);

  std::atomic<StateVersionType> _state_and_version;
};

std::ostream& operator<<(std::ostream& os, const Frame& frame);
}  // namespace hyrise
