#include "join_predicate_ordering_rule.hpp"

#include <algorithm>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace opossum {

void JoinPredicateOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& root) const {
  visit_lqp(root, [&](const auto& node) {
    // Check if this is a multi predicate join.
    if (node->type != LQPNodeType::Join || node->node_expressions.size() <= 1) {
      return LQPVisitation::VisitInputs;
    }

    DebugAssert(std::dynamic_pointer_cast<JoinNode>(node), "LQPNodeType::Join should only be set for JoinNode.");
    const auto join_mode = std::static_pointer_cast<JoinNode>(node)->join_mode;

    DebugAssert(cost_estimator, "JoinOrderingRule requires cost estimator to be set");
    const auto caching_cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();

    // Estimate join selectivity of a predicate by creating a new join node joining only on that one predicate and
    //  estimating that join node's cardinality.
    //  The join selectivity is the ratio "number of tuples in the result/number of tuples in the cartesian product".
    //  See http://www.vldb.org/journal/VLDBJ6/70060191.pdf for more infos on join selectivity.
    auto predicate_cardinalities = std::unordered_map<std::shared_ptr<AbstractExpression>, Cardinality>{};
    for (const auto& predicate : node->node_expressions) {
      const auto single_predicate_join = JoinNode::make(join_mode, predicate, node->left_input(), node->right_input());
      predicate_cardinalities.emplace(predicate,
                                      caching_cardinality_estimator->estimate_cardinality(single_predicate_join));
    }

    // Sort predicates by ascending join selectivity.
    std::sort(node->node_expressions.begin(), node->node_expressions.end(),
              [&](const auto& a, const auto& b) { return predicate_cardinalities[a] < predicate_cardinalities[b]; });

    // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
    //  join predicate. Check that one exists and move it to the front.
    if (join_mode == JoinMode::Semi || join_mode == JoinMode::AntiNullAsTrue ||
        join_mode == JoinMode::AntiNullAsFalse) {
      auto first_equals_predicate = std::find_if(
          node->node_expressions.begin(), node->node_expressions.end(),
          [](const std::shared_ptr<AbstractExpression>& expression) {
            DebugAssert(std::dynamic_pointer_cast<AbstractPredicateExpression>(expression),
                        "Every node expression of a JoinNode should be an AbstractPredicateExpression.");
            return std::static_pointer_cast<AbstractPredicateExpression>(expression)->predicate_condition ==
                   PredicateCondition::Equals;
          });

      Assert(first_equals_predicate != node->node_expressions.end(),
             "Semi/anti joins require at least one equals predicate at the moment.");

      // Shift all predicates before first_equals_predicate back one slot and move first_equals_predicate to the front.
      std::rotate(node->node_expressions.begin(), first_equals_predicate, first_equals_predicate + 1);
    }

    return LQPVisitation::VisitInputs;
  });
}

}  // namespace opossum
