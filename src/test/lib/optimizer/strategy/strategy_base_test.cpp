#include "strategy_base_test.hpp"

#include <memory>
#include <string>
#include <utility>

#include "cost_estimation/cost_estimator_logical.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimator.hpp"

namespace hyrise {

void StrategyBaseTest::_apply_rule(const std::shared_ptr<AbstractRule>& rule, std::shared_ptr<AbstractLQPNode>& input) {
  // Add explicit root node.
  const auto root_node = LogicalPlanRootNode::make();
  root_node->set_left_input(input);

  // Create estimators.
  const auto cardinality_estimator = std::make_shared<CardinalityEstimator>();
  const auto cost_estimator = std::make_shared<CostEstimatorLogical>(cardinality_estimator);
  rule->cost_estimator = cost_estimator;

  rule->apply_to_plan(root_node);

  // The optimizer rules can remove the original input node completely from the plan, so we replace it by the top
  // LQPNode.
  input = root_node->left_input();

  // Remove LogicalPlanRootNode.
  root_node->set_left_input(nullptr);
}

}  // namespace hyrise
