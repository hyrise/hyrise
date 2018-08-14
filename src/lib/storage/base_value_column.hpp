#pragma once

#include "base_column.hpp"

namespace opossum {

/**
 * @brief Super class of value columns
 *
 * Exposes all methods of value columns that do not rely on its specific data type.
 */
class BaseValueColumn : public BaseColumn {
 public:
  using BaseColumn::BaseColumn;

  // returns true if column supports null values
  virtual bool is_nullable() const = 0;

  /**
   * @brief Returns null array
   *
   * Throws exception if is_nullable() returns false
   */
  virtual const pmr_concurrent_vector<bool>& null_values() const = 0;
  virtual pmr_concurrent_vector<bool>& null_values() = 0;
};
}  // namespace opossum
