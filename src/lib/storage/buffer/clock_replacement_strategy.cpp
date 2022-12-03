#include "clock_replacement_strategy.hpp"
#include "utils/assert.hpp"

namespace hyrise {
ClockReplacementStrategy::ClockReplacementStrategy(const size_t num_frames)
    : _num_frames(num_frames), _current_frame_id(0), _num_fram_in_use(0) {
  _pinned_frames.resize(num_frames);
  _reference_bits.resize(num_frames);
};

void ClockReplacementStrategy::record_frame_access(FrameID frame_id) {
  Assert(frame_id < _num_frames, "The recorded frame id is too large for the maxium num of frames");

  // When the frame is pinned, we can just ignore it. Unpinning should happen manually at some other point.
  if (_pinned_frames[frame_id]) {
    return;
  }
  _reference_bits[_current_frame_id] = true;
  _num_fram_in_use++;
}

FrameID ClockReplacementStrategy::find_victim() {
  while (true) {
    if (!_pinned_frames[_current_frame_id]) {
      if (_reference_bits[_current_frame_id]) {
        _reference_bits[_current_frame_id] = false;
      } else {
        _num_fram_in_use--;
        return _current_frame_id;
      }
    }
    _current_frame_id = (_current_frame_id + 1) % _num_frames;
  }

  //return INVALID_FRAME_ID;
  // TODO: Avoid spinning forever if all pages are pinned
}

void ClockReplacementStrategy::pin(FrameID frame_id) {
  _pinned_frames[frame_id] = true;
};

void ClockReplacementStrategy::unpin(FrameID frame_id) {
  _pinned_frames[frame_id] = false;
}

}  // namespace hyrise