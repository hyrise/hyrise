#include "optimizer.hpp"

#include <memory>

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "strategy/join_detection_rule.hpp"
#include "strategy/predicate_reordering_rule.hpp"

namespace opossum {

const Optimizer& Optimizer::get() {
  static Optimizer optimizer;
  return optimizer;
}

Optimizer::Optimizer() {
  _rules.emplace_back(std::make_shared<PredicateReorderingRule>());
  _rules.emplace_back(std::make_shared<JoinDetectionRule>());
}

std::shared_ptr<AbstractLogicalQueryPlanNode> Optimizer::optimize(const std::shared_ptr<AbstractLogicalQueryPlanNode>& input) const {
  // Add explicit root node, so the rules can freely change the tree below it without having to maintain a root node
  // to return to the Optimizer
  const auto root_node = std::make_shared<LogicalPlanRootNode>();
  root_node->set_left_child(input);

  /**
   * Apply all optimization over and over until all of them stopped changing the LQP or the max number of
   * iterations is reached
   */
  for (uint32_t iter_index = 0; iter_index < _max_num_iterations; ++iter_index) {
    auto ast_changed = false;

    for (const auto& rule : _rules) {
      ast_changed |= rule->apply_to(root_node);
    }

    if (!ast_changed) break;
  }

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parents();

  return optimized_node;
}

}  // namespace opossum
