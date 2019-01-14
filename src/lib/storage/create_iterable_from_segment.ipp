#pragma once

#include "reference_segment/reference_segment_iterable.hpp" // NEEDEDINCLUDE

namespace opossum {

template <typename T>
auto create_iterable_from_segment(const ReferenceSegment& segment) {
  if constexpr (HYRISE_DEBUG) {
  	return create_any_segment_iterable<T>(segment);
  } else {
  	return ReferenceSegmentIterable<T>{segment};
  }
}

}  // namespace opossum
