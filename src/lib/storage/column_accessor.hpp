#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "resolve_type.hpp"
#include "storage/reference_column.hpp"
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
  virtual void append(const std::optional<T> value_or_null) = 0;
  virtual ~BaseColumnAccessor() {}
};

/**
 * A ColumnAccessor is templated per ColumnType and DataType (T).
 * It requires implementation of an implicit interface.
 */
template <typename T, typename ColumnType>
class ColumnAccessor : public BaseColumnAccessor<T> {
 public:
  explicit ColumnAccessor(const ColumnType& column) : _column{column} {}

  const std::optional<T> access(ChunkOffset offset) const final { return _column.get_typed_value(offset); }
  void append(const std::optional<T> value_or_null) final { _column.append_typed_value(value_or_null); }

 protected:
  const ColumnType& _column;
};

/**
 * Partial template specialization for ReferenceColumns.
 * Since ReferenceColumns don't know their 'T', this uses the subscript operator as a fallback.
 */
template <typename T>
class ColumnAccessor<T, ReferenceColumn> : public BaseColumnAccessor<T> {
 public:
  explicit ColumnAccessor(const ReferenceColumn& column) : _column{column} {}
  const std::optional<T> access(ChunkOffset offset) const final {
    const auto all_type_variant = _column[offset];
    if (variant_is_null(all_type_variant)) {
      return std::nullopt;
    }
    return type_cast<T>(all_type_variant);
  }

  void append(const std::optional<T> value_or_null) final { Fail("ReferenceColumns are immutable"); }

 protected:
  const ReferenceColumn& _column;
};

/**
 * Utility method to create a ColumnAccessor for a given BaseColumn.
 */
template <typename T>
std::shared_ptr<BaseColumnAccessor<T>> get_column_accessor(const std::shared_ptr<const BaseColumn>& column) {
  std::shared_ptr<BaseColumnAccessor<T>> accessor;
  resolve_column_type<T>(*column, [&](const auto& typed_column) {
    using ColumnType = std::decay_t<decltype(typed_column)>;
    accessor = std::make_shared<ColumnAccessor<T, ColumnType>>(typed_column);
  });
  return accessor;
}

}  // namespace opossum
