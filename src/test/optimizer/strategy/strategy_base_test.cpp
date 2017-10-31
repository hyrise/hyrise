#include "strategy_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "optimizer/abstract_syntax_tree/abstract_ast_node.hpp"
#include "optimizer/abstract_syntax_tree/ast_root_node.hpp"
#include "optimizer/strategy/abstract_rule.hpp"

namespace opossum {

std::shared_ptr<AbstractASTNode> StrategyBaseTest::apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                                              const std::shared_ptr<AbstractASTNode>& input) {
  // Add explicit root node
  const auto root_node = std::make_shared<ASTRootNode>();
  root_node->set_left_child(input);

  rule->apply_to(root_node);

  // Remove ASTRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parents();

  return optimized_node;
}

}  // namespace opossum
