#pragma once

#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/frame_of_reference/frame_of_reference_iterable.hpp"
#include "storage/run_length_segment/run_length_segment_iterable.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

class ReferenceSegment;
template <typename T>
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
 * @{
 */

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const ValueSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return ValueSegmentIterable<T>{segment};
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const DictionarySegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return DictionarySegmentIterable<T, pmr_vector<T>>{segment};
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const RunLengthSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return RunLengthSegmentIterable<T>{segment};
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const FixedStringDictionarySegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return DictionarySegmentIterable<T, FixedStringVector>{segment};
  }
}

template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const FrameOfReferenceSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return FrameOfReferenceIterable<T>{segment};
  }
}

/**
 * This function must be forward-declared because ReferenceSegmentIterable
 * includes this file leading to a circular dependency
 */
template <typename T, bool EraseSegmentType = HYRISE_DEBUG>
auto create_iterable_from_segment(const ReferenceSegment& segment);

/**@}*/

}  // namespace opossum

#include "create_iterable_from_segment.ipp"
