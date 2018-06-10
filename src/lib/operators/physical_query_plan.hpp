#pragma once

#include <unordered_map>

#include "expression/parameter_expression.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

/**
 * Wraps a PQP and its parameters
 */
class PhysicalQueryPlan {
 public:
  PhysicalQueryPlan(const std::shared_ptr<AbstractOperator>& root_op,
                    const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders);

  std::shared_ptr<AbstractOperator> root_op;
  std::unordered_map<ValuePlaceholderID, ParameterID> value_placeholders;
};

}  // namespace opossum
