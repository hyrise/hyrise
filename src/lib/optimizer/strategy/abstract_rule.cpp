#include "abstract_rule.hpp"

#include <memory>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

bool AbstractRule::_apply_to_children(std::shared_ptr<AbstractLQPNode> node) {
  auto children_changed = false;

  // Apply this rule recursively
  if (node->left_input()) {
    children_changed |= apply_to(node->left_input());
  }
  if (node->right_input()) {
    children_changed |= apply_to(node->right_input());
  }

  return children_changed;
}

}  // namespace opossum
