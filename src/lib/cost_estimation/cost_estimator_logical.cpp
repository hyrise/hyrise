#include "cost_estimator_logical.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<AbstractCostEstimator> CostEstimatorLogical::new_instance() const {
  return std::make_shared<CostEstimatorLogical>(cardinality_estimator->new_instance());
}

Cost CostEstimatorLogical::estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
  const auto output_row_count = cardinality_estimator->estimate_cardinality(node);
  const auto left_input_row_count =
      node->left_input() ? cardinality_estimator->estimate_cardinality(node->left_input()) : 0.0f;
  const auto right_input_row_count =
      node->right_input() ? cardinality_estimator->estimate_cardinality(node->right_input()) : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join:
      // Covers predicated and unpredicated joins. For cross joins, output_row_count will be
      // left_input_row_count * right_input_row_count
      return left_input_row_count + right_input_row_count + output_row_count;

    case LQPNodeType::Sort:
      return left_input_row_count * std::log(left_input_row_count);

    case LQPNodeType::Union: {
      const auto union_node = std::static_pointer_cast<UnionNode>(node);

      switch (union_node->union_mode) {
        case UnionMode::Positions:
          return left_input_row_count * std::log(left_input_row_count) +
                 right_input_row_count * std::log(right_input_row_count);
        default:
          Fail("GCC thinks this is reachable");
      }
    }

    case LQPNodeType::Predicate: {
      const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
      return left_input_row_count * _get_expression_cost_multiplier(predicate_node->predicate()) + output_row_count;
    }

    default:
      return left_input_row_count + output_row_count;
  }
}

float CostEstimatorLogical::_get_expression_cost_multiplier(const std::shared_ptr<AbstractExpression>& expression) {
  // Number of operations + number of different columns accessed to factor in expression complexity

  auto multiplier = 0.0f;

  visit_expression(expression, [&](const auto& sub_expression) {
    multiplier += 1.0f;

    if (sub_expression->type == ExpressionType::LQPColumn) {
      multiplier += 1.0f;
    }

    return ExpressionVisitation::VisitArguments;
  });

  return multiplier;
}

}  // namespace opossum
