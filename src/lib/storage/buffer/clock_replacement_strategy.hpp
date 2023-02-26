#pragma once

#include <boost/dynamic_bitset.hpp>
#include <memory>
#include <unordered_set>
#include "frame.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief ClockReplacementStrategy implements the clock or second chance algorithm to select frames
 * for replacement in a FIFO-like fashion. Frames can be pinned to avoid replacement.
 * 
 */
class ClockReplacementStrategy {
 public:
  ClockReplacementStrategy(const size_t num_frames);

  /**
   * @brief Record the usage of a frame to avoid replacement in the near future
   * 
   * @param frame_id FrameID
   */
  void record_frame_access(FrameID frame_id);

  /**
   * @brief Find a FrameID that can be replaced with another page.
   * 
   * @return FrameID FrameID that can be used for replacement
   */
  FrameID find_victim();

  /**
   * @brief Pins a frame that cannot replaced unitl unpinned.
   * 
   * @param frame_id FrameID
   */
  void pin(FrameID frame_id);

  /**
   * @brief Unpin a frame after it was pinned.
   * 
   * @param page_id FrameID
   */
  void unpin(FrameID page_id);

 private:
  // Total number of frame slots
  const size_t _num_frames;

  // Current pointer to a frame
  FrameID _current_frame_id;

  // Frames that are actually used by a clock algorithm
  boost::dynamic_bitset<> _used_frames;

  // Save frames that are pinned and cannot be victimized
  boost::dynamic_bitset<> _pinned_frames;

  // Save which frames where recently accessed and thus get a second-chance
  boost::dynamic_bitset<> _reference_bits;
};

}  // namespace hyrise