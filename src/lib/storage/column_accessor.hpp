#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/base_column_accessor.hpp"
#include "storage/reference_column.hpp"
#include "types.hpp"
#include "utils/performance_warning.hpp"

namespace opossum {

/**
 * A ColumnAccessor is templated per ColumnType and DataType (T).
 * It requires that the underlying column implements an implicit interface:
 *
 *   const std::optional<T> get_typed_value(const ChunkOffset chunk_offset) const;
 *
 */
template <typename T, typename ColumnType>
class ColumnAccessor : public BaseColumnAccessor<T> {
 public:
  explicit ColumnAccessor(const ColumnType& column) : _column{column} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _column.get_typed_value(offset); }

 protected:
  const ColumnType& _column;
};

/**
 * Partial template specialization for ReferenceColumns.
 * Since ReferenceColumns don't know their 'T', this uses the subscript operator as a fallback.
 *
 * Under normal circumstances, using a ColumnAccessor on a ReferenceColumn does not make sense, as
 * it is not faster (potentially even slower) than using operator[] directly on the ReferenceColumn.
 * However, this spezialization is still useful in the case that the underlying column type
 * is not directly known to the calling code and extra code paths for ReferenceColumns should be avoided.
 */
template <typename T>
class ColumnAccessor<T, ReferenceColumn> : public BaseColumnAccessor<T> {
 public:
  explicit ColumnAccessor(const ReferenceColumn& column) : _column{column} {}
  const std::optional<T> access(ChunkOffset offset) const final {
    PerformanceWarning("ColumnAccessor used on ReferenceColumn");
    const auto all_type_variant = _column[offset];
    if (variant_is_null(all_type_variant)) {
      return std::nullopt;
    }
    return type_cast<T>(all_type_variant);
  }

 protected:
  const ReferenceColumn& _column;
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
