#pragma once

#include <boost/dynamic_bitset.hpp>
#include <memory>
#include <unordered_set>
#include "frame.hpp"
#include "types.hpp"

namespace hyrise {

/**
 * @brief 
 * 
 */
class ClockReplacementStrategy : public Noncopyable {
 public:
  ClockReplacementStrategy(const size_t num_frames);

  void record_frame_access(FrameID frame_id);
  FrameID find_victim();

  void pin(FrameID frame_id);
  void unpin(FrameID page_id);

 private:
  const size_t _num_frames;
  FrameID _current_frame_id;
  FrameID _num_fram_in_use;

  // Circular buffer of frame
  boost::dynamic_bitset<> _pinned_frames;
  boost::dynamic_bitset<> _reference_bits;
};

}  // namespace hyrise