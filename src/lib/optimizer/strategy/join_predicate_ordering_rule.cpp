#include "join_predicate_ordering_rule.hpp"

#include <algorithm>
#include <memory>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include "abstract_rule.hpp"
#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/optimization_context.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

void reorder_join_predicates_recursively(const std::shared_ptr<AbstractLQPNode>& node,
                                         const std::shared_ptr<CardinalityEstimator>& cardinality_estimator,
                                         std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!node || !visited_nodes.emplace(node).second) {
    return;
  }

  reorder_join_predicates_recursively(node->left_input(), cardinality_estimator, visited_nodes);
  reorder_join_predicates_recursively(node->right_input(), cardinality_estimator, visited_nodes);

  if (node->type != LQPNodeType::Join || node->node_expressions.size() < 2) {
    return;
  }

  DebugAssert(std::dynamic_pointer_cast<JoinNode>(node), "LQPNodeType::Join should only be set for JoinNode.");
  const auto join_mode = static_cast<const JoinNode&>(*node).join_mode;

  // Estimate join selectivity of a predicate by creating a new join node joining only on that one predicate and
  // estimating that join node's cardinality. The join selectivity is the ratio "number of tuples in the result/number
  // of tuples in the cartesian product".
  // See http://www.vldb.org/journal/VLDBJ6/70060191.pdf for more infos on join selectivity.
  auto& join_predicates = node->node_expressions;
  auto predicate_cardinalities =
      std::unordered_map<std::shared_ptr<AbstractExpression>, Cardinality>{join_predicates.size()};
  for (const auto& predicate : join_predicates) {
    const auto single_predicate_join = JoinNode::make(join_mode, predicate, node->left_input(), node->right_input());
    predicate_cardinalities.emplace(predicate, cardinality_estimator->estimate_cardinality(single_predicate_join));
  }

  // Sort predicates ascending by join selectivity.
  std::sort(join_predicates.begin(), join_predicates.end(), [&](const auto& lhs, const auto& rhs) {
    return predicate_cardinalities[lhs] < predicate_cardinalities[rhs];
  });

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. Check that one exists and move it to the front.
  if (is_semi_or_anti_join(join_mode)) {
    auto first_equals_predicate =
        std::find_if(join_predicates.begin(), join_predicates.end(), [](const auto& expression) {
          DebugAssert(std::dynamic_pointer_cast<AbstractPredicateExpression>(expression),
                      "Every node expression of a JoinNode should be an AbstractPredicateExpression.");
          return std::static_pointer_cast<AbstractPredicateExpression>(expression)->predicate_condition ==
                 PredicateCondition::Equals;
        });

    // SubqueryToJoinRule and JoinToSemiJoinRule should have taken care of that, so this is really just a safeguard.
    Assert(first_equals_predicate != join_predicates.end(),
           "Semi/anti joins require at least one equals predicate at the moment.");

    // Shift all predicates before first_equals_predicate back one slot and move first_equals_predicate to the front.
    std::rotate(join_predicates.begin(), first_equals_predicate, first_equals_predicate + 1);
  }
}

}  // namespace

namespace hyrise {

std::string JoinPredicateOrderingRule::name() const {
  static const auto name = std::string{"JoinPredicateOrderingRule"};
  return name;
}

void JoinPredicateOrderingRule::_apply_to_plan_without_subqueries(const std::shared_ptr<AbstractLQPNode>& lqp_root,
                                                                  OptimizationContext& optimization_context) const {
  // Cache cost of all intermediate operators and joins with reordered predicates.
  DebugAssert(optimization_context.cost_estimator, "JoinOrderingRule requires cost estimator to be set");
  const auto caching_cardinality_estimator = optimization_context.cost_estimator->cardinality_estimator->new_instance();
  caching_cardinality_estimator->guarantee_bottom_up_construction(lqp_root);

  // In theory, the order of join predicates does not make a difference for cardinality estimation. However, we only use
  // the first join predicate to estimate cardinalities. To not massively over-estimate (and to push semi/anti joins as
  // far as possible), the first join predicate should be the one with the smallest selectivity. Because estimations for
  // join predicates further up in the plan depend on the estimates of the nodes below, we must reorder join predicates
  // bottom-up.
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  reorder_join_predicates_recursively(lqp_root, caching_cardinality_estimator, visited_nodes);
}

}  // namespace hyrise
