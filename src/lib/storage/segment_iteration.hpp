#pragma once

#include "pos_list.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"

namespace opossum {

template <typename T, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                            const Functor& functor) {
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
  any_segment_iterable.with_iterators(position_filter, functor);
#else

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
#if IS_DEBUG
  const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
  any_segment_iterable.for_each(position_filter, functor);
#else

#endif
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
