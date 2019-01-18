#pragma once

#include "pos_list.hpp"
#include "resolve_type.hpp"
#include "storage/base_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"
#include "storage/segment_iterables.hpp"
#include "storage/segment_iterables/any_segment_iterable.hpp"

/**
 * This file provides the main entry points to read Segment data, irrespective of the underlying encoding.
 *
 * Two main signatures are provided:
 *      segment_with_iterators[_filtered]()    Calls the functor with a begin and end iterator
 *      segment_iterate[_filtered]()           Calls the functor with each value in the segment
 *
 * The *_filtered() variants of the functions take a PosList wnich allows for selective access to the values in a
 * segment.
 *
 * The template parameter T is either (if known to the caller) the DataType of the values contained in the segment, or
 * ResolveDataTypeTag, if the type is unknown to the caller. ALWAYS pass in the DataType of the Segment if is already
 * known in order to avoid unnecessary code generation.
 *
 * The template parameter EraseTypes specifies if type erasure should be used, which reduces compile
 * time at the cost of run time.
 *
 *
 * ## NOTES REGARDING COMPILE TIME AND BINARY SIZE
 *
 * Calling any of the functions in this file will result in the functor being instantiated many times.
 *   With type erasure:     The functor is instantiated for each DataType
 *   Without type erasure:  The functor is instantiated for each DataType, IterableType and IteratorType combination.
 *
 * Especially when nesting segment iteration, this will lead to a lot of instantiations of the functor, so try to keep
 * them small and use type erasure when performance is not crucial.
 */

namespace opossum {

struct ResolveDataTypeTag {};

// Variant without PosList
template <typename T = ResolveDataTypeTag, EraseTypes erase_iterator_types = EraseTypes::OnlyInDebug, typename Functor>
void segment_with_iterators(const BaseSegment& base_segment, const Functor& functor) {
  if constexpr (std::is_same_v<T, ResolveDataTypeTag>) {
    resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segment_with_iterators<ColumnDataType, erase_iterator_types>(base_segment, functor);
    });
  } else {
    if constexpr (HYRISE_DEBUG || erase_iterator_types == EraseTypes::Always) {
      const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
      any_segment_iterable.with_iterators(functor);
    } else {
      resolve_segment_type<T>(base_segment, [&](const auto& segment) {
        const auto segment_iterable = create_iterable_from_segment<T>(segment);
        segment_iterable.with_iterators(functor);
      });
    }
  }
}

// Variant with PosList
template <typename T = ResolveDataTypeTag, EraseTypes erase_iterator_types = EraseTypes::OnlyInDebug, typename Functor>
void segment_with_iterators_filtered(const BaseSegment& base_segment,
                                     const std::shared_ptr<const PosList>& position_filter, const Functor& functor) {
  if (!position_filter) {
    segment_with_iterators<T, erase_iterator_types>(base_segment, functor);
    return;
  }

  if constexpr (std::is_same_v<T, ResolveDataTypeTag>) {
    resolve_data_type(base_segment.data_type(), [&](const auto data_type_t) {
      using ColumnDataType = typename decltype(data_type_t)::type;
      segment_with_iterators_filtered<ColumnDataType, erase_iterator_types>(base_segment, position_filter, functor);
    });
  } else {
    if constexpr (HYRISE_DEBUG || erase_iterator_types == EraseTypes::Always) {
      const auto any_segment_iterable = create_any_segment_iterable<T>(base_segment);
      any_segment_iterable.with_iterators(position_filter, functor);
    } else {
      resolve_segment_type<T>(base_segment, [&](const auto& segment) {
        const auto segment_iterable = create_iterable_from_segment<T>(segment);
        if constexpr (is_point_accessible_segment_iterable_v<decltype(segment_iterable)>) {
          segment_iterable.with_iterators(position_filter, functor);
        } else {
          Fail("Cannot access non-PointAccessibleSegmentIterable with position_filter");
        }
      });
    }
  }
}

// Variant with PosList
template <typename T = ResolveDataTypeTag, EraseTypes erase_iterator_types = EraseTypes::OnlyInDebug, typename Functor>
void segment_iterate_filtered(const BaseSegment& base_segment, const std::shared_ptr<const PosList>& position_filter,
                              const Functor& functor) {
  segment_with_iterators_filtered<T, erase_iterator_types>(base_segment, position_filter, [&](auto it, const auto end) {
    while (it != end) {
      functor(*it);
      ++it;
    }
  });
}

// Variant without PosList
template <typename T = ResolveDataTypeTag, EraseTypes erase_iterator_types = EraseTypes::OnlyInDebug, typename Functor>
void segment_iterate(const BaseSegment& base_segment, const Functor& functor) {
  segment_with_iterators<T, erase_iterator_types>(base_segment, [&](auto it, const auto end) {
    while (it != end) {
      functor(*it);
      ++it;
    }
  });
}

}  // namespace opossum
