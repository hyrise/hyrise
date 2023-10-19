#pragma once

#include <bit>

#include <atomic>

#include "types.hpp"

namespace hyrise {

/**
 * Frames are the metadata objects for each page. We are using a 64 bit atomic integer to store the latching state, NUMA node, dirty flag and the version of the frame.
 * All operations are atomic. The basic idea and most of the code is based on the SIGMOD'23 paper "Virtual-Memory Assisted Buffer Management" by Leis et al.
 * 
 * The upper 16 bits encoded the state (see below). 1 bit is used for the dirty flag. 7 bits are used for the NUMA node. The lower 40 bits are used for the version. 
 * The version is used to detect concurrent changes to the state of the frame. The version is incremented after unlocking the frame.
 * 
 *  +-----------+-------+-----------+----------------+
 *  | State     | Dirty | NUMA node | Version        |
 *  +-----------+-------+-----------+----------------+
 * 64          48      47          40                0          
 * 
 * The state is encoded as EVICTED, LOCKED, UNLOCKED, MARKED and LOCK_SHARED. Initially, the frame is in state EVICTED. Then, the frame gets LOCKED for reading from disk.
 * After that, the frame is UNLOCKED and can be used. It can be LOCKED again for writing. Or it can be changed into LOCK_SHARED for reading. LOCKED_SHARED can be executed
 * multiple times in parallel. The state MARKED is used for second-chance marking a frame for eviction. The state is changed to EVICTED after the page is written to disk.
 * After unlocking a frame, the version counter is updated. This is used to detect concurrent changes to the state of the frame. The reference implementation can be found
 * in https://github.com/viktorleis/vmcache/blob/master/vmcache.cpp.
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
 */
class Frame final : private Noncopyable {
 public:
  // Type of state and version
  using StateVersionType = uint64_t;

  // UNLOCKED has a value of 0
  static constexpr StateVersionType UNLOCKED = 0;

  // LOCKED_SHARED signifies that the frame has the maxium amount of shared readers. It has a value of 65532.
  static constexpr StateVersionType MAX_LOCKED_SHARED = 0xFFFF - 3;

  // LOCKED signifies that the frame is locked exclusively. It has a value of 65533.
  static constexpr StateVersionType LOCKED = 0xFFFF - 2;

  // MARKED signifies that the frame is marked for eviction. It has a value of 65534.
  static constexpr StateVersionType MARKED = 0xFFFF - 1;

  // EVICTED has a value of 65535 and is the initial state of the frame.
  static constexpr StateVersionType EVICTED = 0xFFFF;

  // Constructs the frame in state EVICTED and version 0.
  Frame();

  // Set the NUMA node of the frame.
  void set_node_id(const NodeID node_id);

  // Set the dirty flag of the frame. Fails if the frame is not locked exclusively.
  void mark_dirty();

  // Check if the frame is dirty.
  bool is_dirty() const;

  // Reset the dirty flag. Fails if the frame is not locked exclusively.
  void reset_dirty();

  // Get the NUMA node of the frame.
  NodeID node_id() const;

  // Try to latch in frame in shared node by incrementing the state. Fails if the frame is not in state UNLOCKED or reaches MAX_LOCKED_SHARED.
  bool try_lock_shared(const StateVersionType old_state_and_version);

  // Try to latch in frame in exclusive node. Fails if the frame is not in state UNLOCKED.
  bool try_lock_exclusive(const StateVersionType old_state_and_version);

  // Try to mark the frame. Fails if the frame is not in state UNLOCKED.
  bool try_mark(const StateVersionType old_state_and_version);

  // Decrement the shared latch. Reaches state UNLOCKED if the latch is 0.
  bool unlock_shared();

  // Unlocks the frame and sets the state to EVICTED.
  void unlock_exclusive_and_set_evicted();

  // Unlocks the frame and increments the version.
  void unlock_exclusive();

  // Check if the frame is in state UNLOCKED.
  bool is_unlocked() const;

  // Load the complete state and version atomically.
  StateVersionType state_and_version() const;

  // Extract the state from the atomic integer. The state is encoded in 16 bits.
  static StateVersionType state(const StateVersionType state_and_version);

  // Extract the version from the atomic integer. The version is encoded in 40 bits.
  static StateVersionType version(const StateVersionType state_and_version);

  // Extract the node id from the atomic integer. The node id is encoded in 4 bits.
  static NodeID node_id(const StateVersionType state_and_version);

 private:
  // clang-format off

  // The dirty flag is encoded in at bit 41.
  static constexpr uint64_t _DIRTY_MASK       = 0x0000800000000000;

  // The NUMA is encoded 7 bits between the state an the the dirty mask.
  static constexpr uint64_t _NODE_ID_MASK     = 0x00007F0000000000;

  // The state is encoded in the upper 16 bits.
  static constexpr uint64_t _STATE_MASK       = 0xFFFF000000000000;

  // The version is encoded in the lower 40 bits.
  static constexpr uint64_t _VERSION_MASK     = 0x000000FFFFFFFFFF;
  // clang-format on

  static_assert((_NODE_ID_MASK ^ _DIRTY_MASK ^ _STATE_MASK ^ _VERSION_MASK) ==
                    std::numeric_limits<StateVersionType>::max(),
                "The given masks either overlap or do not cover the whole StateVersionType.");

  // The number of bits for the state and version. It should be 64.
  static constexpr uint64_t _BIT_WIDTH = sizeof(StateVersionType) * std::numeric_limits<unsigned char>::digits;

  // The number of bits to shift to the right to get the node id.
  static constexpr uint64_t _NODE_ID_SHIFT = std::countr_zero(_NODE_ID_MASK);

  // The number of bits to shift to the right to get the dirty flag.
  static constexpr uint64_t _DIRTY_SHIFT = std::countr_zero(_DIRTY_MASK);

  // The number of bits to shift to the right to get the state.
  static constexpr uint64_t _STATE_SHIFT = std::countr_zero(_STATE_MASK);

  // Update the state and keep the same version. The new state is encoded in the lower 16 bits without the version.
  StateVersionType _update_state_with_same_version(const StateVersionType old_version_and_state,
                                                   const StateVersionType new_state);

  // Update the state and incremente the version. The new state is encoded in the lower 16 bits without the version.
  StateVersionType _update_state_with_increment_version(const StateVersionType old_version_and_state,
                                                        const StateVersionType new_state);

  std::atomic<StateVersionType> _state_and_version;
};

std::ostream& operator<<(std::ostream& os, const Frame& frame);
}  // namespace hyrise
