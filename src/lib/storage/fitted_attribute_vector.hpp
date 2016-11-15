#pragma once

#include <memory>
#include <vector>

#include "../types.hpp"
#include "base_attribute_vector.hpp"

namespace opossum {

// FittedAttributeVector is a specific attribute vector that can hold ValueIDs of variable width (uint8_t, uint16_t,
// uint32_t). A concrete instance can only hold ValueIDs of the same width, but different instances might hold different
// widths
template <typename uintX_t>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  explicit FittedAttributeVector(size_t size) : _attributes(size) {}

  ValueID get(const size_t i) const final { return _attributes[i]; }

  // inserts the value_id at a given position
  void set(const size_t i, const ValueID value_id) final { _attributes[i] = static_cast<uintX_t>(value_id); }

  // returns the number of values
  size_t size() const final { return _attributes.size(); }

 private:
  std::vector<uintX_t> _attributes;
};
}  // namespace opossum
