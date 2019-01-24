#pragma once

#include "resolve_data_type.hpp"
#include "storage/reference_segment.hpp"
#include "storage/resolve_encoded_segment_type.hpp"
#include "storage/value_segment.hpp"

namespace opossum {

/**
 * Given a BaseSegment and its known column type, resolve the segment implementation and call the lambda
 *
 * @param func is a generic lambda or similar accepting a reference to a specialized segment (value, dictionary,
 * reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_segment(ValueSegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(DictionarySegment<T>& segment);
 *
 *   void process_segment(ReferenceSegment& segment);
 *
 *   resolve_segment_type<T>(base_segment, [&](auto& typed_segment) {
 *     process_segment(typed_segment);
 *   });
 */
template <typename In, typename Out>
using ConstOutIfConstIn = std::conditional_t<std::is_const_v<In>, const Out, Out>;

template <typename ColumnDataType, typename BaseSegmentType, typename Functor>
// BaseSegmentType allows segment to be const and non-const
std::enable_if_t<std::is_same_v<BaseSegment, std::remove_const_t<BaseSegmentType>>>
/*void*/ resolve_segment_type(BaseSegmentType& segment, const Functor& func) {
  using ValueSegmentPtr = ConstOutIfConstIn<BaseSegmentType, ValueSegment<ColumnDataType>>*;
  using ReferenceSegmentPtr = ConstOutIfConstIn<BaseSegmentType, ReferenceSegment>*;
  using EncodedSegmentPtr = ConstOutIfConstIn<BaseSegmentType, BaseEncodedSegment>*;

  if (auto value_segment = dynamic_cast<ValueSegmentPtr>(&segment)) {
    func(*value_segment);
  } else if (auto ref_segment = dynamic_cast<ReferenceSegmentPtr>(&segment)) {
    func(*ref_segment);
  } else if (auto encoded_segment = dynamic_cast<EncodedSegmentPtr>(&segment)) {
    resolve_encoded_segment_type<ColumnDataType>(*encoded_segment, func);
  } else {
    Fail("Unrecognized column type encountered.");
  }
}

/**
 * Resolves a data type by passing a hana::type object and the downcasted segment on to a generic lambda
 *
 * @param data_type is an enum value of any of the supported column types
 * @param func is a generic lambda or similar accepting two parameters: a hana::type object and
 *   a reference to a specialized segment (value, dictionary, reference)
 *
 *
 * Example:
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, ValueSegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, DictionarySegment<T>& segment);
 *
 *   template <typename T>
 *   void process_segment(hana::basic_type<T> type, ReferenceSegment& segment);
 *
 *   resolve_data_and_segment_type(base_segment, [&](auto type, auto& typed_segment) {
 *     process_segment(type, typed_segment);
 *   });
 */
template <typename Functor, typename BaseSegmentType>  // BaseSegmentType allows segment to be const and non-const
std::enable_if_t<std::is_same_v<BaseSegment, std::remove_const_t<BaseSegmentType>>>
/*void*/ resolve_data_and_segment_type(BaseSegmentType& segment, const Functor& func) {
  resolve_data_type(segment.data_type(), [&](auto type) {
    using ColumnDataType = typename decltype(type)::type;

    resolve_segment_type<ColumnDataType>(segment, [&](auto& typed_segment) { func(type, typed_segment); });
  });
}

}  // namespace opossum
