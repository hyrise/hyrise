#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "types.hpp"

namespace opossum {

/**
 * This is the base class for all ColumnAccessor types.
 * It provides the common interface to access individual values of a column.
 */
template <typename T>
class BaseColumnAccessor {
 public:
  virtual const std::optional<T> access(ChunkOffset offset) const = 0;
  virtual ~BaseColumnAccessor() {}
};

/**
 * Utility method to create a ColumnAccessor for a given BaseColumn.
 */
template <typename T>
std::unique_ptr<BaseColumnAccessor<T>> create_column_accessor(const std::shared_ptr<const BaseColumn>& column) {
  std::unique_ptr<BaseColumnAccessor<T>> accessor;
  resolve_column_type<T>(*column, [&](const auto& typed_column) {
    using ColumnType = std::decay_t<decltype(typed_column)>;
    accessor = std::make_unique<ColumnAccessor<T, ColumnType>>(typed_column);
  });
  return accessor;
}

}  // namespace opossum
