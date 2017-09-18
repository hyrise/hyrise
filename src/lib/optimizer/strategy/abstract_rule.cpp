#include "abstract_rule.hpp"

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"

namespace opossum {

void AbstractRule::apply_to_children(const std::shared_ptr<AbstractASTNode> &node) {
  // Apply this rule recursively
  if (node->left_child()) {
    apply_to(node->left_child());
  }
  if (node->right_child()) {
    apply_to(node->right_child());
  }
}

}