#pragma once

#include <unordered_map>

#include "expression/parameter_expression.hpp"

#include "types.hpp"

namespace opossum {

/**
 * Allocates ParameterIDs for ValuePlaceholders and correlated expressions during SQL translation
 */
class ParameterIDAllocator {
 public:
  ParameterID allocate();
  ParameterID allocate_for_value_placeholder(const ValuePlaceholderID value_placeholder);

  const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders() const;

 private:
  ParameterID _parameter_id_counter{0};
  std::unordered_map<ValuePlaceholderID, ParameterID> _value_placeholders;
};

}  // namespace opossum
