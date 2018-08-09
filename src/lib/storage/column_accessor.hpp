#pragma once

#include <memory>
#include <optional>
#include <type_traits>

#include "types.hpp"
#include "resolve_type.hpp"

namespace opossum {

template <typename T>
class BaseAccessor {
public:
  virtual const std::optional<T> access(ChunkOffset offset) = 0;
  virtual ~BaseAccessor() {}
};

template <typename T, typename ColumnType>
class ColumnAccessor : public BaseAccessor<T> {
public:
  explicit ColumnAccessor(const ColumnType& column) : _column{column} { }

  const std::optional<T> access(ChunkOffset offset) final {
    return _column.get_typed_value(offset);
  }
protected:
  const ColumnType& _column;
};

template <typename T>
class ColumnAccessor<T, ReferenceColumn> : public BaseAccessor<T> {
public:
  explicit ColumnAccessor(const ReferenceColumn& column) {}
  const std::optional<T> access(ChunkOffset offset) final {
    Fail("Cannot use ColumnAccessor on a ReferenceColumn.");
  }
};

template <typename T>
std::shared_ptr<BaseAccessor<T>> get_column_accessor(const std::shared_ptr<BaseColumn>& column) {
  std::shared_ptr<BaseAccessor<T>> accessor;
  resolve_column_type<T>(*column, [&](auto& typed_column) {
     using ColumnType = std::decay_t<decltype(typed_column)>;
     accessor = std::make_shared<ColumnAccessor<T, ColumnType>>(typed_column);
   });
   return accessor;
}


} // namespace opossum
