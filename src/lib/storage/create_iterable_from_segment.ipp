#pragma once

#include "reference_segment/reference_segment_iterable.hpp" // NEEDEDINCLUDE

namespace opossum {

template <typename T>
auto create_iterable_from_segment(const ReferenceSegment& segment) {
  return erase_type_from_iterable_if_debug(ReferenceSegmentIterable<T>{segment});
}

}  // namespace opossum
