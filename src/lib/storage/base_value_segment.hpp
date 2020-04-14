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

  // returns true if segment supports null values
  virtual bool is_nullable() const = 0;

  // appends the value at the end of the segment
  virtual void append(const AllTypeVariant& val) = 0;

  /**
   * @brief Returns vector of NULL values (which is true for offsets where the segment's value is NULL).
   *        Cannot be written to, see value_segment.hpp for details.
   *
   * Throws exception if is_nullable() returns false
   */
  virtual const pmr_vector<bool>& null_values() const = 0;
};
}  // namespace opossum
