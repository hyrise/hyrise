#pragma once

#include <boost/dynamic_bitset.hpp>
#include <memory>
#include <unordered_set>
#include "frame.hpp"
#include "storage/buffer/types.hpp"

namespace hyrise {

/**
 * @brief ClockReplacementStrategy implements the clock or second chance algorithm to select frames
 * for replacement in a FIFO-like fashion.
 * 
 */
class ClockReplacementStrategy {
 public:
  ClockReplacementStrategy(std::vector<Frame>& frames);

  /**
   * @brief Record the usage of a frame to avoid replacement in the near future
   * 
   * @param frame_id FrameID
   */
  void record_frame_access(const FrameID frame_id);

  /**
   * @brief Find a FrameID that can be replaced with another page.
   * 
   * @return FrameID FrameID that can be used for replacement
   */
  Frame* find_victim();

 private:
  // Current pointer to a frame
  FrameID _current_frame_id;

  std::vector<Frame>& _frames;
};

}  // namespace hyrise