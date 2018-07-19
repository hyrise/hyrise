#pragma once

#include "storage/base_column.hpp"
#include "types.hpp"

namespace opossum {

// BaseColumnT is the super class for all column types which can be referenced by
// a ReferenceColumn. The ReferenceColumnIterable is using this class.

template <typename T>
class BaseTypedColumn : public virtual BaseColumn {
 public:
  using BaseColumn::BaseColumn;
  // returns a pair which has a boolean and a value of type T.
  // The boolean is true if the value is NULL.
  virtual const std::pair<T, bool> get_typed_value(const ChunkOffset chunk_offset) const = 0;
};
}  // namespace opossum
