#pragma once

#include "pos_list.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

struct ResolveDataTypeTag {};

template <typename T = ResolveDataTypeTag, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const Functor& functor) {
  if constexpr (std::is_same_v<T, ResolveDataTypeTag>) {
    resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segment_with_iterators<ColumnDataType>(base_segment, functor);
    });
  } else {
#if IS_DEBUG
    const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
    any_segment_iterable.with_iterators(functor);
#else
    resolve_segment_type<T>(base_segment, [&](const auto& segment) {
      const auto segment_iterable = create_iterable_from_segment<T>(segment);
      segment_iterable.with_iterators(functor);
    });
#endif
  }
}

template <typename T = ResolveDataTypeTag, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                            const Functor& functor) {
  if (!position_filter) {
    segment_with_iterators<T>(base_segment, functor);
    return;
  }

  if constexpr (std::is_same_v<T, ResolveDataTypeTag>) {
    resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segment_with_iterators<ColumnDataType>(base_segment, position_filter, functor);
    });
  } else {
#if IS_DEBUG 
    const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
    any_segment_iterable.with_iterators(position_filter, functor);
#else
    resolve_segment_type<T>(base_segment, [&](const auto& segment) {
      const auto segment_iterable = create_iterable_from_segment<T>(segment);
      if constexpr (is_point_accessible_segment_iterable_v<decltype(segment_iterable)>) {
        segment_iterable.with_iterators(position_filter, functor);
      } else {
        Fail("Cannot access non-PointAccessibleSegmentIterable with position_filter");
      }
    });
#endif
  }
}

template <typename T = ResolveDataTypeTag, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                      const Functor& functor) {
  segment_with_iterators<T>(base_segment, position_filter, [&](auto it, const auto end) {
    while (it != end) {
      functor(*it);
      ++it;
    }
  });
}

template <typename T = ResolveDataTypeTag, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const Functor& functor) {
  segment_with_iterators<T>(base_segment, [&](auto it, const auto end) {
    while (it != end) {
      functor(*it);
      ++it;
    }
  });
}

}  // namespace opossum
