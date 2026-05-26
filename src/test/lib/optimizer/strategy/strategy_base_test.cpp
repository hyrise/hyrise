#include "strategy_base_test.hpp"

#include <memory>

#include "cost_estimation/cost_estimator_logical.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

void StrategyBaseTest::SetUp() {
  _optimization_context = OptimizationContext{};
  _optimization_context.cost_estimator =
      std::make_shared<CostEstimatorLogical>(std::make_shared<CardinalityEstimator>());
}

void StrategyBaseTest::_apply_rule(const std::shared_ptr<AbstractRule>& rule, std::shared_ptr<AbstractLQPNode>& input) {
  // Make sure there is no reference to the input as an uncopied expected plan.
  Assert(input.use_count() == 1, "LQP is referenced multiple times. Did you mean to make a deep copy?");

  // Add explicit root node.
  const auto root_node = LogicalPlanRootNode::make();
  root_node->set_left_input(input);

  rule->apply_to_plan(root_node, _optimization_context);

  // The optimizer rules can remove the original input node completely from the plan, so we replace it by the top
  // LQPNode.
  input = root_node->left_input();

  // Remove LogicalPlanRootNode.
  root_node->set_left_input(nullptr);
}

}  // namespace hyrise
