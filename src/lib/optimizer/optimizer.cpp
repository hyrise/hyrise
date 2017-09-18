#include "optimizer.hpp"

#include <memory>

#include "abstract_syntax_tree/ast_root_node.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"

namespace opossum {

const Optimizer &Optimizer::get() {
  static Optimizer optimizer;
  return optimizer;
}

Optimizer::Optimizer() {
  _rules.emplace_back(std::make_shared<PredicateReorderingRule>());
  _rules.emplace_back(std::make_shared<JoinConditionDetectionRule>());
}

std::shared_ptr<AbstractASTNode> Optimizer::optimize(const std::shared_ptr<AbstractASTNode> &input) const {
  // Add explicit root node
  const auto root_node = std::make_shared<ASTRootNode>();
  root_node->set_left_child(input);

  // Apply all optimization over and over until all of them stopped changing the AST
  while (true) {
    auto ast_changed = false;

    for (const auto &rule : _rules) {
      if (rule->apply_to(root_node)) {
        ast_changed = true;
      }
    }

    if (!ast_changed) break;
  }

  // Remove ASTRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parent();

  return optimized_node;
}

}  // namespace opossum
