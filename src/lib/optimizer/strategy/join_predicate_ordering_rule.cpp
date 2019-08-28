#include "join_predicate_ordering_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/logical_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace opossum {

void JoinPredicateOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Check if this is a multi predicate join.
  if (node->type != LQPNodeType::Join || node->node_expressions.size() <= 1) {
    _apply_to_inputs(node);
    return;
  }

  const auto join_mode = std::static_pointer_cast<JoinNode>(node)->join_mode;

  DebugAssert(cost_estimator, "JoinOrderingRule requires cost estimator to be set");
  const auto caching_cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();

  // Estimate selectivity of a predicate by getting cardinalities for a join node joining only on that one predicate.
  auto predicate_cardinalities = std::unordered_map<std::shared_ptr<AbstractExpression>, Cardinality>{};
  for (const auto& predicate : node->node_expressions) {
    const auto single_predicate_join = JoinNode::make(join_mode, predicate);
    single_predicate_join->set_left_input(node->left_input());
    single_predicate_join->set_right_input(node->right_input());

    predicate_cardinalities[predicate] = caching_cardinality_estimator->estimate_cardinality(single_predicate_join);
    std::cout << predicate->as_column_name() << " est. card.: " << predicate_cardinalities[predicate] << '\n';  // TODO remove
  }

  // Sort predicates by descending selectivity.
  std::sort(node->node_expressions.begin(), node->node_expressions.end(),
            [&](const std::shared_ptr<AbstractExpression>& a, const std::shared_ptr<AbstractExpression>& b) {
              return predicate_cardinalities[a] < predicate_cardinalities[b];
            });

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. Check that one exists and move it to the front.
  if (join_mode == JoinMode::Semi || join_mode == JoinMode::AntiNullAsTrue || join_mode == JoinMode::AntiNullAsFalse) {
    auto first_equals_predicate =
        std::find_if(node->node_expressions.begin(), node->node_expressions.end(),
                     [](const std::shared_ptr<AbstractExpression>& expression) {
                       return std::static_pointer_cast<AbstractPredicateExpression>(expression)->predicate_condition ==
                              PredicateCondition::Equals;
                     });

    Assert(first_equals_predicate != node->node_expressions.end(),
           "Semi/anti joins require at least one equals predicate at the moment.");

    while (first_equals_predicate != node->node_expressions.begin()) {
      std::iter_swap(first_equals_predicate, first_equals_predicate - 1);
      first_equals_predicate -= 1;
    }

    Assert(std::static_pointer_cast<AbstractPredicateExpression>(node->node_expressions.front())->predicate_condition ==
               PredicateCondition::Equals,
           "The primary join predicate must be equals for semi/anti join.");
  }

  _apply_to_inputs(node);
}

}  // namespace opossum
