#include "abstract_rule.hpp" // NEEDEDINCLUDE

#include "logical_query_plan/abstract_lqp_node.hpp" // NEEDEDINCLUDE

namespace opossum {

void AbstractRule::_apply_to_inputs(std::shared_ptr<AbstractLQPNode> node) const {  // NOLINT
  if (node->left_input()) apply_to(node->left_input());
  if (node->right_input()) apply_to(node->right_input());
}

}  // namespace opossum
