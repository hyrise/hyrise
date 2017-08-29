#pragma once

#include "tbb/concurrent_vector.h"

#include "base_column.hpp"

namespace opossum {

/**
 * @brief Super class of value columns
 *
 * Exposes all methods of value columns that do not rely on its specific data type.
 */
class BaseValueColumn : public BaseColumn {
 public:
  // returns if columns supports null values
  virtual bool is_nullable() const = 0;

  /**
   * @brief Returns null array
   *
   * Throws exception if is_nullable() returns false
   */
  virtual const tbb::concurrent_vector<bool>& null_values() const = 0;
  virtual tbb::concurrent_vector<bool>& null_values() = 0;
};
}  // namespace opossum
