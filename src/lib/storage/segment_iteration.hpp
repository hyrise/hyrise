#pragma once

#include "pos_list.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

template <typename T, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                            const Functor& functor) {
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
  any_segment_iterable.with_iterators(position_filter, functor);
#else
  resolve_segment_type<T>(base_segment, [&](const auto& segment) {
    const auto segment_iterable = create_iterable_from_segment<T>(segment);
    if (position_filter) {
      using SegmentType = typename std::decay_t<decltype(segment)>;

      if constexpr (is_point_accessible_segment_iterable_v<SegmentType>) {
        segment_iterable.with_iterators(position_filter, functor);
      } else {
        Fail("Cannot access non-PointAccessibleSegmentIterable with position_filter"); 
      }
    } else {
      segment_iterable.with_iterators(functor);
    }
  });
#endif
}

template <typename T, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const Functor& functor) {
  segment_with_iterators<T>(base_segment, nullptr, functor);
}

template <typename Functor>
void segment_with_iterators_and_data_type_resolve(const BaseSegment& base_segment,
                                                  const std::shared_ptr<const PosList>& position_filter,
                                                  const Functor& functor) {
  resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    segment_with_iterators<ColumnDataType>(base_segment, position_filter, functor);
  });
}

template <typename Functor>
void segment_with_iterators_and_data_type_resolve(const BaseSegment& base_segment, const Functor& functor) {
  segment_with_iterators_and_data_type_resolve(base_segment, nullptr, functor);
}

template <typename T, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                      const Functor& functor) {
  segment_with_iterators<T>(base_segment, position_filter, [&](auto it, const auto end) {
    while (it != end) {
      functor(*it);
      ++it;
    }
  });
}

template <typename T, typename Functor>
void segment_for_each(const BaseSegment& base_segment, const Functor& functor) {
  segment_for_each<T>(base_segment, nullptr, functor);
}

template <typename Functor>
void segment_for_each_and_data_type_resolve(const BaseSegment& base_segment,
                                            const std::shared_ptr<const PosList>& position_filter,
                                            const Functor& functor) {
  resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
    using ColumnDataType = typename decltype(data_type_t)::type;
    segment_for_each<ColumnDataType>(base_segment, position_filter, functor);
  });
}

template <typename Functor>
void segment_for_each_and_data_type_resolve(const BaseSegment& base_segment, const Functor& functor) {
  segment_for_each_and_data_type_resolve(base_segment, nullptr, functor);
}

}  // namespace opossum
