#pragma once

#include "types.hpp"

#include <atomic>
#include <bit>

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

class Frame final {
 public:
  using StateVersionType = uint64_t;

  // State constants
  static constexpr StateVersionType UNLOCKED = 0;
  static constexpr StateVersionType LOCKED_SHARED = 0xFFFF - 3;  // 252 if 8 bits, 65532
  static constexpr StateVersionType LOCKED = 0xFFFF - 2;         // 253 if 8 bits, 65533
  static constexpr StateVersionType MARKED = 0xFFFF - 1;         // 254 if 8 bits, 65534
  static constexpr StateVersionType EVICTED = 0xFFFF;            // 255 if 8 bits, 65535 for 16 bits

  Frame();

  // Flags and other metadata
  void set_node_id(const NodeID node_id);
  void set_dirty(const bool new_dirty);
  bool is_dirty() const;
  void reset_dirty();
  NodeID node_id() const;

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
  static NodeID node_id(StateVersionType state_and_version);

  void _debug_print();

 private:
  // clang-format off
  static constexpr uint64_t NODE_ID_MASK   = 0x00000F0000000000;
  static constexpr uint64_t DIRTY_MASK       = 0x0000F00000000000;
  static constexpr uint64_t STATE_MASK       = 0xFFFF000000000000;
  static constexpr uint64_t VERSION_MASK     = 0x000000FFFFFFFFFF;
  // clang-format on
  static_assert((NODE_ID_MASK ^ DIRTY_MASK ^ STATE_MASK ^ VERSION_MASK) == std::numeric_limits<StateVersionType>::max(),
                "The given masks do not cover the whole StateVersionType");

  static constexpr uint64_t NUM_BITS = sizeof(StateVersionType) * CHAR_BIT;
  static constexpr uint64_t NODE_ID_SHIFT = std::countr_zero(NODE_ID_MASK);
  static constexpr uint64_t DIRTY_SHIFT = std::countr_zero(DIRTY_MASK);
  static constexpr uint64_t STATE_SHIFT = std::countr_zero(STATE_MASK);

  StateVersionType update_state_with_same_version(StateVersionType old_version_and_state, StateVersionType new_state);
  StateVersionType update_state_with_increment_version(StateVersionType old_version_and_state,
                                                       StateVersionType new_state);

  std::atomic<StateVersionType> _state_and_version;
};

std::ostream& operator<<(std::ostream& os, const Frame& frame);
}  // namespace hyrise
