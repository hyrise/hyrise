#include "abstract_rule.hpp"

#include <memory>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_root_node.hpp"

namespace opossum {

std::shared_ptr<AbstractASTNode> AbstractRule::apply_rule(const std::shared_ptr<AbstractRule> & rule,
                                                              const std::shared_ptr<AbstractASTNode> &input) {
//  PerformanceWarning("Optimizations should be done by the Optimizer");

  // Add explicit root node
  const auto root_node = std::make_shared<ASTRootNode>();
  root_node->set_left_child(input);

  //
  rule->apply_to(root_node);

  // Remove ASTRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parent();

  return optimized_node;
}

bool AbstractRule::_apply_to_children(std::shared_ptr<AbstractASTNode> node) {
  auto result = false;

  // Apply this rule recursively
  if (node->left_child()) {
    result |= apply_to(node->left_child());
  }
  if (node->right_child()) {
    result |= apply_to(node->right_child());
  }

  return result;
}

}  // namespace opossum
