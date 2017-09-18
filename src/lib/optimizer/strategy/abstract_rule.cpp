#include "abstract_rule.hpp"

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_root_node.hpp"

namespace opossum {

const std::shared_ptr<AbstractASTNode> AbstractRule::apply_to(const std::shared_ptr<AbstractASTNode> &node) {
  // Add explicit root node
  const auto root_node = std::make_shared<ASTRootNode>();
  root_node->set_left_child(node);

  _apply_to_impl(root_node);

  // Remove ASTRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parent();

  return optimized_node;
}

void AbstractRule::_apply_to_children(const std::shared_ptr<AbstractASTNode> &node) {
  // Apply this rule recursively
  if (node->left_child()) {
    _apply_to_impl(node->left_child());
  }
  if (node->right_child()) {
    _apply_to_impl(node->right_child());
  }
}

}  // namespace opossum
