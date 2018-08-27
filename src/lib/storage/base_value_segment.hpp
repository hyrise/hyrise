#pragma once

#include "base_segment.hpp"

namespace opossum {

/**
 * @brief Super class of value segments
 *
 * Exposes all methods of value segments that do not rely on its specific data type.
 */
class BaseValueSegment : public BaseSegment {
 public:
  using BaseSegment::BaseSegment;

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
