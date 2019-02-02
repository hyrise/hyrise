#include "expression_reduction_rule.hpp"

#include <functional>
#include <unordered_set>

#include "expression/expression_functional.hpp"
#include "expression/in_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

using namespace opossum::expression_functional;  // NOLINT

std::string ExpressionReductionRule::name() const { return "Expression Reduction Rule"; }

void ExpressionReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  Assert(node->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto");

  visit_lqp(node, [&](const auto& sub_node) {
    for (auto& expression : sub_node->node_expressions) {
      visit_expression(expression, [&](auto& sub_expression) {
        // TODO(anybody) Unnecessary shared_ptr copies being made here.
        expression = reduce_distributivity(expression);
        expression = rewrite_in_with_single_list_element(expression);

        if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(expression)) {
          apply_to(subquery_expression->lqp);
        }

        return ExpressionVisitation::VisitArguments;
      });
    }

    return LQPVisitation::VisitInputs;
  });

}

std::shared_ptr<AbstractExpression> ExpressionReductionRule::reduce_distributivity(
    const std::shared_ptr<AbstractExpression>& input_expression) {
  // Step 1: `(a AND b AND c) OR (a AND d AND b AND e)` --> `[(a AND b AND c), (a AND d AND b AND e)]`
  const auto flat_disjunction = flatten_logical_expressions(input_expression, LogicalOperator::Or);

  // Step 2: `[(a AND b AND c), (a AND d AND b AND e)]` --> `[[a, b, c], [a, d, b, e]]`
  auto flat_disjunction_and_conjunction = std::vector<std::vector<std::shared_ptr<AbstractExpression>>>{};
  flat_disjunction_and_conjunction.reserve(flat_disjunction.size());

  for (const auto& expression : flat_disjunction) {
    flat_disjunction_and_conjunction.emplace_back(flatten_logical_expressions(expression, LogicalOperator::And));
  }

  // Step 3: Identify common_conjunctions: [a, b]
  auto common_conjunctions = flat_disjunction_and_conjunction.front();

  for (auto conjunction_idx = size_t{1}; conjunction_idx < flat_disjunction_and_conjunction.size(); ++conjunction_idx) {
    const auto& flat_conjunction = flat_disjunction_and_conjunction[conjunction_idx];

    for (auto common_iter = common_conjunctions.begin(); common_iter != common_conjunctions.end();) {
      if (std::find_if(flat_conjunction.begin(), flat_conjunction.end(), [&](const auto& expression) {
            return *expression == *(*common_iter);
          }) == flat_conjunction.end()) {
        common_iter = common_conjunctions.erase(common_iter);
      } else {
        ++common_iter;
      }
    }
  }

  // Step 4: Remove common_conjunctions from flat_disjunction_and_conjunction.
  //         flat_disjunction_and_conjunction = [[c], [d, e]]
  for (auto& flat_conjunction : flat_disjunction_and_conjunction) {
    for (auto expression_iter = flat_conjunction.begin(); expression_iter != flat_conjunction.end();) {
      if (std::find_if(common_conjunctions.begin(), common_conjunctions.end(), [&](const auto& expression) {
            return *expression == *(*expression_iter);
          }) != common_conjunctions.end()) {
        expression_iter = flat_conjunction.erase(expression_iter);
      } else {
        ++expression_iter;
      }
    }
  }

  // Step 5: Rebuild inflated expression from conjunctions in flat_disjunction_and_conjunction:
  //         `[[c], [d, e]]` --> `[c, (d AND e)]`
  auto flat_disjunction_remainder = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& flat_conjunction : flat_disjunction_and_conjunction) {
    if (!flat_conjunction.empty()) {
      const auto inflated_conjunction = inflate_logical_expressions(flat_conjunction, LogicalOperator::And);
      flat_disjunction_remainder.emplace_back(inflated_conjunction);
    }
  }

  // Step 6: Rebuild inflated expression from flat_disjunction_remainder:
  //          `[c, (d AND e)]` --> `c OR (d AND e)`
  auto inflated_disjunction_remainder = std::shared_ptr<AbstractExpression>{};
  if (!flat_disjunction_remainder.empty()) {
    inflated_disjunction_remainder = inflate_logical_expressions(flat_disjunction_remainder, LogicalOperator::Or);
  }

  // Step 7: Rebuild inflated expression from common_conjunction: `[a, c]` --> `(a AND c)`
  auto common_conjunction_expression = inflate_logical_expressions(common_conjunctions, LogicalOperator::And);

  // Step 8: Build result expression from common_conjunction_expression and inflated_disjunction_remainder:
  //         `(a AND c)` AND `c OR (d AND e)`
  if (common_conjunction_expression && inflated_disjunction_remainder) {
    return and_(common_conjunction_expression, inflated_disjunction_remainder);
  } else {
    if (common_conjunction_expression) return common_conjunction_expression;
    Assert(inflated_disjunction_remainder, "Bug detected. inflated_disjunction_remainder should contain an expression");
    return inflated_disjunction_remainder;
  }
}

std::shared_ptr<AbstractExpression> ExpressionReductionRule::rewrite_in_with_single_list_element(
const std::shared_ptr<AbstractExpression>& input_expression) {
  if (const auto in_expression = std::dynamic_pointer_cast<InExpression>(input_expression)) {
    if (const auto list_expression = std::dynamic_pointer_cast<ListExpression>(in_expression)) {
       if (list_expression->arguments.size() == 1) {
         return equals_(in_expression->value(), list_expression->arguments[0]);
       }
    }
  }

  // No rewriteable expression, just return the original expression
  return input_expression;
}

}  // namespace opossum
