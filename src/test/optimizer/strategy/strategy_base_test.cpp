#include "strategy_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "logical_query_plan/abstract_logical_query_plan_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "optimizer/strategy/abstract_rule.hpp"

namespace opossum {

std::shared_ptr<AbstractLogicalQueryPlanNode> StrategyBaseTest::apply_rule(const std::shared_ptr<AbstractRule>& rule,
                                                              const std::shared_ptr<AbstractLogicalQueryPlanNode>& input) {
  // Add explicit root node
  const auto root_node = std::make_shared<LogicalPlanRootNode>();
  root_node->set_left_child(input);

  rule->apply_to(root_node);

  // Remove LogicalPlanRootNode
  const auto optimized_node = root_node->left_child();
  optimized_node->clear_parents();

  return optimized_node;
}

}  // namespace opossum
