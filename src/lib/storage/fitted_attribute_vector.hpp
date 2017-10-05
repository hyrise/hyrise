#pragma once

#include <limits>
#include <memory>
#include <vector>

#include "base_attribute_vector.hpp"

#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

/**
 * FittedAttributeVector is a specific attribute vector that
 * can hold ValueIDs of variable width (uint8_t, uint16_t, uint32_t).
 * A concrete instance can only hold ValueIDs of the same width,
 * but different instances might hold different widths.
 *
 * Note: Because in most cases NULL_VALUE_ID cannot be stored inside uintX_t,
 *       it is represented by max(uintX_t)
 */

template <typename uintX_t>
class FittedAttributeVector : public BaseAttributeVector {
 public:
  static constexpr auto CLAMPED_NULL_VALUE_ID = std::numeric_limits<uintX_t>::max();

 public:
  explicit FittedAttributeVector(size_t size) : _attributes(size) {}
  explicit FittedAttributeVector(size_t size, const PolymorphicAllocator<uintX_t>& alloc) : _attributes(size, alloc) {}

  // Creates a FittedAttributeVector from given attributes
  explicit FittedAttributeVector(const pmr_vector<uintX_t>& attributes) : _attributes(attributes) {}

  /**
   * Returns the ValueID for a given record
   * Note: max(uintX_t) is converted to NULL_VALUE_ID
   */
  ValueID get(const size_t i) const final {
    auto value_id = _attributes[i];
    return (value_id == CLAMPED_NULL_VALUE_ID) ? NULL_VALUE_ID : static_cast<ValueID>(value_id);
  }

  /**
   * Inserts the value_id at a given position
   * Note: NULL_VALUE_ID is converted to max(uintX_t)
   */
  void set(const size_t i, const ValueID value_id) final {
    DebugAssert(value_id < CLAMPED_NULL_VALUE_ID || value_id == NULL_VALUE_ID, "value_id to large to fit into uintX_t");
    _attributes[i] = static_cast<uintX_t>(value_id);
  }

  // returns all attributes
  const pmr_vector<uintX_t>& attributes() const { return _attributes; }

  // returns the number of values
  size_t size() const final { return _attributes.size(); }

  AttributeVectorWidth width() const final { return sizeof(uintX_t); }

 private:
  pmr_vector<uintX_t> _attributes;
};
}  // namespace opossum
