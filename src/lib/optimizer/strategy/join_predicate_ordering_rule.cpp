#include <expression/logical_expression.hpp>
#include "join_predicate_ordering_rule.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"
#include "expression/expression_utils.hpp"

#include "operators/print.hpp"

// TODO remove cout stuff

namespace opossum {

void JoinPredicateOrderingRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  DebugAssert(cost_estimator, "JoinOrderingRule requires cost estimator to be set");

  if (node->type != LQPNodeType::Join || node->node_expressions.size() <= 1) {
    // not a multi predicate join
    _apply_to_inputs(node);
    return;
  }

  const auto caching_cardinality_estimator = cost_estimator->cardinality_estimator->new_instance();

  std::cout << "found a multi predicate join with these predicates and predicted cardinalities:\n";
  auto predicate_cardinalities = std::unordered_map<std::shared_ptr<AbstractExpression>, Cardinality>{};
  for (const auto& predicate : node->node_expressions) {
    auto single_predicate_join = JoinNode::make(JoinMode::Left, predicate);
    single_predicate_join->set_left_input(node->left_input());
    single_predicate_join->set_right_input(node->right_input());

    predicate_cardinalities[predicate] = caching_cardinality_estimator->estimate_cardinality(single_predicate_join);
    std::cout << predicate->as_column_name() << ": " << predicate_cardinalities[predicate] << "\n";
  }

  std::sort(node->node_expressions.begin(), node->node_expressions.end(),
            [&](const std::shared_ptr<AbstractExpression>& a, const std::shared_ptr<AbstractExpression>& b){
    return predicate_cardinalities[a] < predicate_cardinalities[b];
  });

  // TODO ensure the first predicate is equals

  _apply_to_inputs(node);
}

}  // namespace opossum
