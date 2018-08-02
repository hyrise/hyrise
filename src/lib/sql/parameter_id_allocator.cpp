#include "parameter_id_allocator.hpp"

namespace opossum {

ParameterID ParameterIDAllocator::allocate() { return static_cast<ParameterID>(_parameter_id_counter++); }

ParameterID ParameterIDAllocator::allocate_for_value_placeholder(const ValuePlaceholderID value_placeholder_id) {
  const auto parameter_id = allocate();
  const auto is_unique = _value_placeholders.emplace(value_placeholder_id, parameter_id).second;
  Assert(is_unique, "Duplicate ValuePlaceholderID");

  return parameter_id;
}

const std::unordered_map<ValuePlaceholderID, ParameterID>& ParameterIDAllocator::value_placeholders() const {
  return _value_placeholders;
}

}  // namespace opossum
