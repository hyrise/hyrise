#pragma once

#include <optional>

#include "resolve_type.hpp"
#include "storage/abstract_segment.hpp"
#include "storage/create_iterable_from_segment.hpp"

namespace opossum {

/**
 * @brief Materialization convenience functions.
 *
 * Use like:
 *
 * ```c++
 *   pmr_vector<std::optional<T>> values_and_nulls;
 *   values_and_nulls.reserve(chunk->size()); // Optional
 *   materialize_values_and_nulls(*chunk->get_segment(expression->column_id()), values_and_nulls);
 *   return values_and_nulls;
 * ```
 */

// Materialize the values in the segment
template <typename Container>
void materialize_values(const AbstractSegment& segment, Container& container) {
  using ContainerValueType = typename Container::value_type;

  resolve_segment_type<ContainerValueType>(segment, [&](const auto& typed_segment) {
    create_iterable_from_segment<ContainerValueType>(typed_segment).materialize_values(container);
  });
}

// Materialize the values/nulls in the segment
template <typename Container>
void materialize_values_and_nulls(const AbstractSegment& segment, Container& container) {
  using ContainerValueType = typename Container::value_type::second_type;

  resolve_segment_type<ContainerValueType>(segment, [&](const auto& typed_segment) {
    create_iterable_from_segment<ContainerValueType>(typed_segment).materialize_values_and_nulls(container);
  });
}

// Materialize the nulls in the segment
template <typename SegmentValueType, typename Container>
void materialize_nulls(const AbstractSegment& segment, Container& container) {
  resolve_segment_type<SegmentValueType>(segment, [&](const auto& typed_segment) {
    create_iterable_from_segment<SegmentValueType>(typed_segment).materialize_nulls(container);
  });
}

}  // namespace opossum
