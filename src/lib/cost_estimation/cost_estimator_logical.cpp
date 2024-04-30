#include "cost_estimator_logical.hpp"

#include <algorithm>
#include <cmath>
#include <memory>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/expression_utils.hpp"
#include "expression/list_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace {

using namespace hyrise;  // NOLINT(build/namespaces)

float expression_cost_multiplier(const std::shared_ptr<AbstractExpression>& expression) {
  auto multiplier = 0.0f;

  // Determine the number of columns accessed by the predicate to factor in expression complexity. Also add a factor for
  // correlated subqueries as we have to evaluate the subquery for each tuple again.
  // We start with a factor of 0 and continuously add 1 for each column to ease, e.g., the estimation of column vs.
  // column predicates of nested predicates ('SELECT ... WHERE column_a = column_b OR column_c = 4` needs to evaluate
  // three columns).
  // Returning the maximum of `multiplier` and 1 accounts for tautologies (`SELECT ... WHERE 1 = 1`), which we currently
  // do not optimize and pass to the ExpressionEvaluator.
  //
  // In the past, we added to the factor for each expression in the predicate. This resulted in too pessimistic cost
  // estimations for PredicateNodes compared to (semi-)joins, which turned out to be a problem when we switched to
  // cost-based predicate ordering (see PredicateReorderingRule) in #2590. For example, the predicate `column_c = 4`
  // would end up with a factor of 4 (1 for the binary predicate, 1 for each argument of the predicate, and 1 additional
  // for the LQPColumnExpression), making its cost way higher than the cost of a semi-join with worse selectivity.
  visit_expression(expression, [&](const auto& sub_expression) {
    if (sub_expression->type == ExpressionType::LQPColumn ||
        (sub_expression->type == ExpressionType::LQPSubquery &&
         static_cast<LQPSubqueryExpression&>(*sub_expression).is_correlated())) {
      multiplier += 1.0f;
      // We do not return here. Thus, we continue to add a penalty for each parameter of a correlated subquery. We tried
      // adding the subplan cost here, as well as ignoring the number of parameters. Both approaches did not yield
      // satisfactory benchmark performance, see #2590.
      // LQPColumnExpressions do not have arguments anyway, so we just save an if branch and do continue for them, too.
    } else if (sub_expression->type == ExpressionType::List) {
      // ListExpressions can have many elements, all of which should be values or simple operations. Thus, we do not
      // visit all of them separately as they cannot increase the multiplier.
      if constexpr (HYRISE_DEBUG) {
        for (const auto& list_element : static_cast<const ListExpression&>(*sub_expression).elements()) {
          const auto element_is_column_like = list_element->type == ExpressionType::LQPColumn ||
                                              (sub_expression->type == ExpressionType::LQPSubquery &&
                                               static_cast<LQPSubqueryExpression&>(*sub_expression).is_correlated());
          Assert(!element_is_column_like, "Did not expect columns or correlated subqueries in ListExpression.");
        }
      }

      return ExpressionVisitation::DoNotVisitArguments;
    }

    return ExpressionVisitation::VisitArguments;
  });

  return std::max(1.0f, multiplier);
}

}  // namespace

namespace hyrise {

std::shared_ptr<AbstractCostEstimator> CostEstimatorLogical::new_instance() const {
  return std::make_shared<CostEstimatorLogical>(cardinality_estimator->new_instance());
}

Cost CostEstimatorLogical::estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node,
                                              const bool cacheable) const {
  const auto output_row_count = cardinality_estimator->estimate_cardinality(node, cacheable);
  const auto left_input_row_count =
      node->left_input() ? cardinality_estimator->estimate_cardinality(node->left_input(), cacheable) : 0.0f;
  const auto right_input_row_count =
      node->right_input() ? cardinality_estimator->estimate_cardinality(node->right_input(), cacheable) : 0.0f;

  switch (node->type) {
    case LQPNodeType::Join:
      // Covers predicated and unpredicated joins. For cross joins, output_row_count will be
      // left_input_row_count * right_input_row_count.
      return left_input_row_count + right_input_row_count + output_row_count;

    case LQPNodeType::Sort:
      // n * log(n) for sorting, plus n for output writing.
      return left_input_row_count * std::log(left_input_row_count) + output_row_count;

    case LQPNodeType::Union: {
      const auto union_mode = static_cast<const UnionNode&>(*node).set_operation_mode;

      switch (union_mode) {
        case SetOperationMode::Positions:
          // To merge the PosLists, we have to sort them. Thus, n * log(n) for each input plus output writing.
          return left_input_row_count * std::log(left_input_row_count) +
                 right_input_row_count * std::log(right_input_row_count) + output_row_count;
        case SetOperationMode::All:
          // UnionAll simply appends its two inputs and does not touch the actual data.
          return 0.0f;
        case SetOperationMode::Unique:
          Fail("ToDo, see discussion https://github.com/hyrise/hyrise/pull/2156#discussion_r452803825");
      }

      Fail("This cannot happen, but gcc thinks this is a fall-through and complains.");
    }

    case LQPNodeType::StoredTable:
      // Simply forwards segments, does not touch the data.
      return 0.0f;

    case LQPNodeType::Predicate: {
      const auto& predicate = static_cast<const PredicateNode&>(*node).predicate();
      // n * number of scanned columns + output writing.
      return left_input_row_count * expression_cost_multiplier(predicate) + output_row_count;
    }

    default:
      return left_input_row_count + output_row_count;
  }
}

}  // namespace hyrise
