#pragma once

#include "types.hpp"

namespace opossum {

template <typename T>
class ValueSegment;

template <typename T>
class DictionarySegment;

template <typename T>
class RunLengthSegment;

template <typename T>
class FixedStringDictionarySegment;

template <typename T, typename>
class FrameOfReferenceSegment;

template <typename T>
class LZ4Segment;

class ReferenceSegment;
template <typename T, EraseReferencedSegmentType>
class ReferenceSegmentIterable;

/**
 * @defgroup Uniform interface to create an iterable from a segment
 *
 * These methods cannot be part of the segments' interfaces because
 * reference segment are not templated and thus don’t know their type.
 *
 * All iterables implement the same interface using static polymorphism
 * (i.e. the CRTP pattern, see segment_iterables/.hpp).
 *
 * In debug mode, create_iterable_from_segment returns a type erased
 * iterable, i.e., all iterators have the same type
 *
 * Functions must be forward-declared because otherwise, we run into
 * circular include dependencies.
 *
 * @{
 */

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const ValueSegment<T>& segment);

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const DictionarySegment<T>& segment);

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const RunLengthSegment<T>& segment);

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const FixedStringDictionarySegment<T>& segment);

template <typename T, typename Enabled, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const FrameOfReferenceSegment<T, Enabled>& segment);

// Fix template deduction so that we can call `create_iterable_from_segment<T, false>` on FrameOfReferenceSegments
template <typename T, bool EraseSegmentType, typename Enabled>
auto create_iterable_from_segment(const FrameOfReferenceSegment<T, Enabled>& segment) {
  return create_iterable_from_segment<T, Enabled, EraseSegmentType>(segment);
}

template <typename T, bool EraseSegmentType = true>
auto create_iterable_from_segment(const LZ4Segment<T>& segment);

template <typename T, bool EraseSegmentType = HYRISE_DEBUG,
          EraseReferencedSegmentType = (HYRISE_DEBUG ? EraseReferencedSegmentType::Yes
                                                     : EraseReferencedSegmentType::No)>
auto create_iterable_from_segment(const ReferenceSegment& segment);

/**@}*/

}  // namespace opossum

// Include these only now to break up include dependencies
#include "create_iterable_from_reference_segment.ipp"
#include "create_iterable_from_segment.ipp"
