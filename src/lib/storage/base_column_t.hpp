#pragma once

#include "types.hpp"

namespace opossum {

// BaseColumn is the abstract super class for all column types,
// e.g., ValueColumn, ReferenceColumn

template <typename T>
class BaseColumnT {
 public:
  // returns the value at a given position
  virtual const T get_t(const ChunkOffset chunk_offset) const = 0;
};
}  // namespace opossum
