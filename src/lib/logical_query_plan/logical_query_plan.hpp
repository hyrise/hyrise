#pragma once

#include <unordered_map>

#include "expression/parameter_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

/**
 * Wraps an LQP and its parameters
 */
class LogicalQueryPlan {
 public:
  LogicalQueryPlan(const std::shared_ptr<AbstractLQPNode>& root_node, const std::unordered_map<ValuePlaceholderID, ParameterID>& value_placeholders);

  std::shared_ptr<AbstractLQPNode> root_node;
  std::unordered_map<ValuePlaceholderID, ParameterID> value_placeholders;
};

}  // namespace opossum
