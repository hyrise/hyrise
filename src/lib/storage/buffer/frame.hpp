#pragma once

#include <atomic>

#include <bit>

#include "types.hpp"

namespace hyrise {

/**
 * Frames are the metadata objects for each page. We are using a 64 bit atomic integer to store the (latching) state, NUMA node, dirty flag and the version of the frame.
 * All operations are atomic. The basic idea and most of the code is based on the SIGMOD'23 paper "Virtual-Memory Assisted Buffer Management" by Leis et al.
 * 
 * The frame's upper 16 bits encode the (latching) state (see below). 1 bit is used for the dirty flag. 7 bits are used for the NUMA node. The lower 40 bits are used for the version. 
 * The version is used to detect concurrent changes to the state of the frame. The version is incremented after unlocking the frame.
 * 
 *  +-----------+-------+-----------+----------------+
 *  | State     | Dirty | NUMA node | Version        |
 *  +-----------+-------+-----------+----------------+
 * 64          48      47          40                0          
 * 
 * The (latching) state is encoded as EVICTED (65535), LOCKED (65533), UNLOCKED (0), MARKED (65534) and LOCK_SHARED (1-65532). Initially, the frame is in state EVICTED. 
 * Then, the frame gets LOCKED for reading from disk. After that, the frame is UNLOCKED and can be used. It can be LOCKED again for write access. For multiple current readers,
 * the state is incremented by 1 until MAX_LOCKED_SHARED is reached. When locking in shared mode, we also need to perform the same amount of unlocked to move the state to UNLOCKED again.  
 * The state MARKED is used for Second-Chance marking a frame for eviction. Only frames that were previously MARKED after the UNLOCKED state are eligible for eviction. This approximates an LRU policy. 
 * Later, the state is changed to EVICTED after the page has been written to disk. After unlocking a frame, the version counter is updated. The version is used to detect concurrent changes to the 
 * state of the frame. The version is primarily used for the Second-Chance eviction mechanism. We use it to verify if an enqueued frame has been modified in the meantime and thereby is outdated. If so,
 * we know that a newer version of the frame has been enqueued or that the frame is currently locked. Thus, we can skip the frame for now. The version can also be used to implementet an optimistic latching 
 * mechanism.
 * 
 * The reference implementation can be found in https://github.com/viktorleis/vmcache/blob/master/vmcache.cpp.
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
 *                                                                                    | (only if no other latches are present)                     |
 *                                                                                    +------------------------------------------------------------+
 */
class Frame final : private Noncopyable {
 public:
  // Type of state and version
  using StateVersionType = uint64_t;

  // UNLOCKED has a value of 0
  static constexpr StateVersionType UNLOCKED = 0;

  // SINGLE_LOCKED_SHARED signifies that the frame has a single reader. It has a value of 1. Used for convenience.
  static constexpr StateVersionType SINGLE_LOCKED_SHARED = 1;

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

  /**
   * Try to latch in frame in shared node by incrementing the state. Fails if the frame is 
   * not in state UNLOCKED, MARKED or reaches MAX_LOCKED_SHARED.
   */
  bool try_lock_shared(const StateVersionType old_state_and_version);

  /**
   * Try to latch in frame in exclusive node. Fails if the frame is not in state UNLOCKED or MARKED. 
   * Returns true on success.
   */
  bool try_lock_exclusive(const StateVersionType old_state_and_version);

  // Try to mark the frame. Fails if the frame is not in state UNLOCKED. Returns true on success.
  bool try_mark(const StateVersionType old_state_and_version);

  // Decrement the shared latch. Returns true if the last shared latch has been released (state is UNLOCKED).
  bool unlock_shared();

  // Unlocks the frame after an exclusive lock and sets the state to EVICTED.
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

  // Extract the node id from the atomic integer. The node id is encoded in 7 bits.
  static NodeID node_id(const StateVersionType state_and_version);

 private:
  // clang-format off

  // The dirty flag is encoded in at bit 41.
  static constexpr uint64_t _dirty_mask       = 0x0000800000000000;

  // The NUMA is encoded 7 bits between the state an the the dirty mask.
  static constexpr uint64_t _node_id_mask     = 0x00007F0000000000;

  // The state is encoded in the upper 16 bits.
  static constexpr uint64_t _state_mask       = 0xFFFF000000000000;

  // The version is encoded in the lower 40 bits.
  static constexpr uint64_t _version_mask     = 0x000000FFFFFFFFFF;

  // clang-format on

  static_assert((_node_id_mask ^ _dirty_mask ^ _state_mask ^ _version_mask) ==
                    std::numeric_limits<StateVersionType>::max(),
                "The given masks either overlap or do not cover the whole StateVersionType.");

  // The number of bits for the state and version. It should be 64.
  static constexpr uint64_t _bit_width = std::numeric_limits<StateVersionType>::digits;

  // The number of bits to shift to the right to get the node id.
  static constexpr uint64_t _node_id_shift = std::countr_zero(_node_id_mask);

  // The number of bits to shift to the right to get the dirty flag.
  static constexpr uint64_t _dirty_shift = std::countr_zero(_dirty_mask);

  // The number of bits to shift to the right to get the state.
  static constexpr uint64_t _state_shift = std::countr_zero(_state_mask);

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
