#pragma once

#include "reference_segment/reference_segment_iterable.hpp"

namespace opossum {

template <typename T, bool EraseType>
auto create_iterable_from_segment(const ReferenceSegment& segment) {
  if constexpr (EraseType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return ReferenceSegmentIterable<T>{segment};
  }
}

}  // namespace opossum
