#include "abstract_rule.hpp"

#include <memory>

#include "expression/expression_utils.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"

namespace opossum {

void AbstractRule::_apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const {  // NOLINT
  if (node->input_left()) apply_to(node->input_left());
  if (node->input_right()) apply_to(node->input_right());
}

}  // namespace opossum
