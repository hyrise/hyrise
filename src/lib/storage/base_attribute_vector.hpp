#pragma once

#include "../types.hpp"

namespace opossum {

// BaseAttributeVector is the abstract super class for all attribute vectors,
// e.g., FittedAttributeVector
class BaseAttributeVector {
 public:
  BaseAttributeVector() = default;

  // copying an attribute vector is not allowed
  // copying whole attribute vectors is expensive
  BaseAttributeVector(BaseAttributeVector const &) = delete;
  BaseAttributeVector &operator=(const BaseAttributeVector &) = delete;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseAttributeVector(BaseAttributeVector &&) = default;
  BaseAttributeVector &operator=(BaseAttributeVector &&) = default;

  virtual ValueID get(const size_t i) const = 0;

  // inserts the value_id at a given position
  virtual void set(const size_t i, const ValueID value_id) = 0;

  // returns the number of values
  virtual size_t size() const = 0;
};
}  // namespace opossum
