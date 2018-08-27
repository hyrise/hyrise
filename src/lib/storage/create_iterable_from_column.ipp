#pragma once

#include "reference_segment/reference_segment_iterable.hpp"

namespace opossum {

template <typename T>
auto create_iterable_from_column(const ReferenceSegment& column) {
  return erase_type_from_iterable_if_debug(ReferenceSegmentIterable<T>{column});
}

}  // namespace opossum
