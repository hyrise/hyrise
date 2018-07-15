#pragma once

#include <optional>

#include "resolve_type.hpp"
#include "storage/base_column.hpp"
#include "storage/create_iterable_from_column.hpp"

namespace opossum {

/**
 * @brief Materialization convenience functions.
 *
 * Use like:
 *
 * ```c++
 *   pmr_vector<std::optional<T>> values_and_nulls;
 *   values_and_nulls.reserve(chunk->size()); // Optional
 *   materialize_values_and_nulls(*chunk->get_column(expression->column_id()), values_and_nulls);
 *   return values_and_nulls;
 * ```
 */

// Materialize the values in the Column
template <typename Container>
void materialize_values(const BaseColumn& column, Container& container) {
  using ContainerValueType = typename Container::value_type;

  resolve_column_type<ContainerValueType>(column, [&](const auto& column) {
    create_iterable_from_column<ContainerValueType>(column).materialize_values(container);
  });
}

// Materialize the values/nulls in the Column
template <typename Container>
void materialize_values_and_nulls(const BaseColumn& column, Container& container) {
  using ContainerValueType = typename Container::value_type::second_type;
  resolve_column_type<ContainerValueType>(column, [&](const auto& column) {
    create_iterable_from_column<ContainerValueType>(column).materialize_values_and_nulls(container);
  });
}

// Materialize the nulls in the Column
template <typename ColumnValueType, typename Container>
void materialize_nulls(const BaseColumn& column, Container& container) {
  resolve_column_type<ColumnValueType>(column, [&](const auto& column) {
    create_iterable_from_column<ColumnValueType>(column).materialize_nulls(container);
  });
}

}  // namespace opossum
