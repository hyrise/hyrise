#pragma once

#include <memory>

#include "types.hpp"

namespace opossum {

// BaseAttributeVector is the abstract super class for all attribute vectors,
// e.g., FittedAttributeVector
class BaseAttributeVector : private Noncopyable {
 public:
  BaseAttributeVector() = default;
  virtual ~BaseAttributeVector() = default;

  // we need to explicitly set the move constructor to default when
  // we overwrite the copy constructor
  BaseAttributeVector(BaseAttributeVector&&) = default;
  BaseAttributeVector& operator=(BaseAttributeVector&&) = default;

  virtual ValueID get(const size_t i) const = 0;

  // inserts the value_id at a given position
  virtual void set(const size_t i, const ValueID value_id) = 0;

  // returns the number of values
  virtual size_t size() const = 0;

  // returns the width of the values in bytes
  virtual AttributeVectorWidth width() const = 0;

  virtual std::shared_ptr<BaseAttributeVector> migrate(const PolymorphicAllocator<size_t>& alloc) const = 0;
};
}  // namespace opossum
