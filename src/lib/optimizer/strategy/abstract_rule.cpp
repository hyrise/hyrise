#include "abstract_rule.hpp"

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

bool AbstractRule::_apply_to_children(std::shared_ptr<AbstractASTNode> node) {
  auto children_changed = false;

  // Apply this rule recursively
  if (node->left_child()) {
    children_changed |= apply_to(node->left_child());
  }
  if (node->right_child()) {
    children_changed |= apply_to(node->right_child());
  }

  return children_changed;
}

}  // namespace opossum
