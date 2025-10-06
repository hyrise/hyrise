#pragma once

#include "types.hpp"
#include "create_iterable_from_non_reference_segment.hpp"

namespace hyrise {

class ReferenceSegment;
template <typename T, EraseReferencedSegmentType>
class ReferenceSegmentIterable;

template <typename T, bool erase_segment_type = HYRISE_DEBUG,
          EraseReferencedSegmentType = (HYRISE_DEBUG ? EraseReferencedSegmentType::Yes
                                                     : EraseReferencedSegmentType::No)>
auto create_iterable_from_segment(const ReferenceSegment& segment);

}  // namespace hyrise

// Include these only now to break up include dependencies
#include "create_iterable_from_reference_segment.ipp"
