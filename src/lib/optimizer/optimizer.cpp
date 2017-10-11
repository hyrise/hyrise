#include "optimizer.hpp"

#include <memory>

#include "abstract_syntax_tree/ast_root_node.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"
#include "utils/ast_printer.hpp"

namespace opossum {

const Optimizer& Optimizer::get() {
  static Optimizer optimizer;
  return optimizer;
}

Optimizer::Optimizer() {
  _rules.emplace_back(std::make_shared<PredicateReorderingRule>());
  _rules.emplace_back(std::make_shared<JoinConditionDetectionRule>());
}

std::shared_ptr<AbstractASTNode> Optimizer::optimize(const std::shared_ptr<AbstractASTNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = std::make_shared<ASTRootNode>();
  root_node->set_left_child(input);

//  std::cout << "Optimizing:" << std::endl;
//  ASTPrinter::print(root_node);

  /**
   * Apply all optimization over and over until all of them stopped changing the AST or the max number of
   * iterations is reached
   */
  for (uint32_t iter_index = 0; iter_index < _max_num_iterations; ++iter_index) {
    auto ast_changed = false;

    for (const auto& rule : _rules) {
//      std::cout << "Applying Rule '" << rule->name() << "'" << std::endl;
      ast_changed |= rule->apply_to(root_node);
//      ASTPrinter::print(root_node);
    }

    if (!ast_changed) break;
  }

  // Remove ASTRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parent();

  return optimized_node;
}

}  // namespace opossum
