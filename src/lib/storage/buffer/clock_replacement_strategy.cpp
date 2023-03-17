#include "clock_replacement_strategy.hpp"
#include "utils/assert.hpp"

// TODO: Allow removal of frame

namespace hyrise {
ClockReplacementStrategy::ClockReplacementStrategy(std::vector<Frame>& frames) : _frames(frames) {}

void ClockReplacementStrategy::record_frame_access(const FrameID frame_id) {
  Assert(frame_id < _frames->capacity(), "frame_id cannot be larger than the total number of frames");
  auto frame = _frames[frame_id];

  frame.referenced = true;
  frame.in_use = true;
}

Frame* ClockReplacementStrategy::find_victim() {
  // Assert(!_pinned_frames.all(), "All frames are currently pinned");

  // Avoid an endless-loop if most frames are pinned. In this case, it should return an INVALID_FRAME_ID
  // and the buffer manager should handle things itself. We loop at most 2 times, so the the reference-bits
  // can be reset after one round.
  const auto num_frames = _frames.size();
  for (auto frames_left = num_frames * 2; frames_left > 0;
       frames_left--, _current_frame_id = (_current_frame_id + 1) % num_frames) {
    auto frame = _frames[_current_frame_id];

    if (frame.pin_count.load() > 0) {
      continue;
    }

    if (!frame.in_use) {
      continue;
    }

    DebugAssert(frame.data != nullptr, "Data should be set");

    if (frame.referenced) {
      // Remove the reference bit. This frame could become a victim in the next round.
      frame.referenced = false;
    } else {
      // Victimize this frame and remove it from the clock
      return &frame;
    }
  }

  return nullptr;
}

}  // namespace hyrise