#pragma once

#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
#include "storage/frame_of_reference_segment/frame_of_reference_segment_iterable.hpp"
#include "storage/lz4_segment/lz4_segment_iterable.hpp"
#include "storage/run_length_segment/run_length_segment_iterable.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/value_segment/value_segment_iterable.hpp"

namespace opossum {

template <typename T, bool EraseSegmentType>
auto create_iterable_from_segment(const ValueSegment<T>& segment) {
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return ValueSegmentIterable<T>{segment};
  }
}

template <typename T, bool EraseSegmentType>
auto create_iterable_from_segment(const DictionarySegment<T>& segment) {
#ifdef HYRISE_ERASE_DICTIONARY
  PerformanceWarning("DictionarySegmentIterable erased by compile-time setting");
  return AnySegmentIterable<T>(DictionarySegmentIterable<T, pmr_vector<T>>(segment));
#else
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return DictionarySegmentIterable<T, pmr_vector<T>>{segment};
  }
#endif
}

template <typename T, bool EraseSegmentType>
auto create_iterable_from_segment(const RunLengthSegment<T>& segment) {
#ifdef HYRISE_ERASE_RUNLENGTH
  PerformanceWarning("RunLengthSegmentIterable erased by compile-time setting");
  return AnySegmentIterable<T>(RunLengthSegmentIterable<T>(segment));
#else
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return RunLengthSegmentIterable<T>{segment};
  }
#endif
}

template <typename T, bool EraseSegmentType>
auto create_iterable_from_segment(const FixedStringDictionarySegment<T>& segment) {
#ifdef HYRISE_ERASE_FIXEDSTRINGDICTIONARY
  PerformanceWarning("FixedStringDictionarySegmentIterable erased by compile-time setting");
  return AnySegmentIterable<T>(DictionarySegmentIterable<T, FixedStringVector>(segment));
#else
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return DictionarySegmentIterable<T, FixedStringVector>{segment};
  }
#endif
}

template <typename T, typename Enabled, bool EraseSegmentType>
auto create_iterable_from_segment(const FrameOfReferenceSegment<T, Enabled>& segment) {
#ifdef HYRISE_ERASE_FRAMEOFREFERENCE
  PerformanceWarning("FrameOfReferenceSegmentIterable erased by compile-time setting");
  return AnySegmentIterable<T>(FrameOfReferenceSegmentIterable<T>(segment));
#else
  if constexpr (EraseSegmentType) {
    return create_any_segment_iterable<T>(segment);
  } else {
    return FrameOfReferenceSegmentIterable<T>{segment};
  }
#endif
}

template <typename T, bool EraseSegmentType>
auto create_iterable_from_segment(const LZ4Segment<T>& segment) {
  // LZ4Segment always gets erased as its decoding is so slow, the virtual function calls won't make
  // a difference. If we'd allow it to not be erased we'd risk compile time increase creeping in for no benefit
  return AnySegmentIterable<T>(LZ4SegmentIterable<T>(segment));
}

}  // namespace opossum
