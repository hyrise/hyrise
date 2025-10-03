#pragma once

#include "storage/reference_segment/reference_segment_iterable.hpp"

namespace hyrise {

template <typename T, bool erase_segment_type, EraseReferencedSegmentType erase_referenced_segment_type>
auto create_iterable_from_segment(const ReferenceSegment& segment) {
  if constexpr (erase_segment_type) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return ReferenceSegmentIterable<T, erase_referenced_segment_type>{segment};
  }
}

}  // namespace hyrise
