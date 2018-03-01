#pragma once

#include <optional>

#include "storage/base_column.hpp"
#include "storage/create_iterable_from_column.hpp"
#include "resolve_type.hpp"

namespace opossum {

/**
 * Materialization convenience functions. Can't be put into base_column.hpp because that would lead to circular
 * includes.
 *
 * Use like:
 *
 * pmr_concurrent_vector<std::optional<T>> values_and_nulls;
 * values_and_nulls.reserve(chunk->size()); // Optional
 * materialize_values_and_nulls(*chunk->get_column(expression->column_id()), values_and_nulls);
 * return values_and_nulls;
 */

// Materialize the values in the Column
template <template <typename> typename Container, typename T>
void materialize_values(const BaseColumn& column, Container<T>& container) {
  resolve_column_type<T>(
      column, [&](const auto& column) { create_iterable_from_column<T>(column).materialize_values(container); });
}

// Materialize the values/nulls in the Column
template <template <typename> typename Container, typename T>
void materialize_values_and_nulls(const BaseColumn& column, Container<std::optional<T>>& container) {
  resolve_column_type<T>(column, [&](const auto& column) {
    create_iterable_from_column<T>(column).materialize_values_and_nulls(container);
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
