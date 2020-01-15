#pragma once

#include "storage/dictionary_segment/dictionary_segment_iterable.hpp"
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

}  // namespace opossum
