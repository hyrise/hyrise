#include "expression_reduction_rule.hpp"

#include <functional>
#include <unordered_set>

#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
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
      reduce_distributivity(expression);
      reduce_in_with_single_list_element(expression);

      // We can't prune Aggregate arguments, because the operator doesn't support, e.g., `MIN(1)`, whereas it supports
      // `MIN(2-1)`, since `2-1` becomes a column.
      if (sub_node->type != LQPNodeType::Aggregate) {
        // TODO(anybody) We can't prune top level expressions right now, because that breaks `SELECT MIN(1+2)...`
        //               because if we rewrite that to `SELECT MIN(3)...` the input to the aggregate is not a column
        //               anymore and the Aggregate operator cannot handle that
        for (auto& argument : expression->arguments) {
          reduce_constant_expression(argument);
        }
      }
    }

    return LQPVisitation::VisitInputs;
  });
}

const std::shared_ptr<AbstractExpression>& ExpressionReductionRule::reduce_distributivity(
    std::shared_ptr<AbstractExpression>& input_expression) {
  // Step 1: `(a AND b AND c) OR (a AND d AND b AND e)` --> `[(a AND b AND c), (a AND d AND b AND e)]`
  const auto flat_disjunction = flatten_logical_expressions(input_expression, LogicalOperator::Or);

  // Step 2: `[(a AND b AND c), (a AND d AND b AND e)]` --> `[[a, b, c], [a, d, b, e]]`
  auto flat_disjunction_and_conjunction = std::vector<std::vector<std::shared_ptr<AbstractExpression>>>{};
  flat_disjunction_and_conjunction.reserve(flat_disjunction.size());

  for (const auto& expression : flat_disjunction) {
    flat_disjunction_and_conjunction.emplace_back(flatten_logical_expressions(expression, LogicalOperator::And));
  }

  // Step 3 Recurse into `a, b, c, d, e` and look for possible reductions there. Do this here because we have
  //        `flat_disjunction_and_conjunction` handily available here (it gets modified later).
  if (flat_disjunction_and_conjunction.size() == 1u && flat_disjunction_and_conjunction.front().size() == 1u) {
    // input_expression is neither AND nor OR - continue with its argument
    for (auto& argument : input_expression->arguments) {
      reduce_distributivity(argument);
    }
  } else {
    // input_expression is a nesting of AND and/or OR - continue with the arguments to those
    for (auto& sub_expressions : flat_disjunction_and_conjunction) {
      for (auto& sub_expression : sub_expressions) {
        reduce_distributivity(sub_expression);
      }
    }
  }

  // Step 4: Identify common_conjunctions: [a, b]
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

  // Step 5: Remove common_conjunctions from flat_disjunction_and_conjunction.
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

  // Step 6: Rebuild inflated expression from conjunctions in flat_disjunction_and_conjunction:
  //         `[[c], [d, e]]` --> `[c, (d AND e)]`
  auto flat_disjunction_remainder = std::vector<std::shared_ptr<AbstractExpression>>{};

  for (const auto& flat_conjunction : flat_disjunction_and_conjunction) {
    if (!flat_conjunction.empty()) {
      const auto inflated_conjunction = inflate_logical_expressions(flat_conjunction, LogicalOperator::And);
      flat_disjunction_remainder.emplace_back(inflated_conjunction);
    }
  }

  // Step 7: Rebuild inflated expression from flat_disjunction_remainder:
  //          `[c, (d AND e)]` --> `c OR (d AND e)`
  auto inflated_disjunction_remainder = std::shared_ptr<AbstractExpression>{};
  if (!flat_disjunction_remainder.empty()) {
    inflated_disjunction_remainder = inflate_logical_expressions(flat_disjunction_remainder, LogicalOperator::Or);
  }

  // Step 8: Rebuild inflated expression from common_conjunction: `[a, c]` --> `(a AND c)`
  auto common_conjunction_expression = inflate_logical_expressions(common_conjunctions, LogicalOperator::And);

  // Step 9: Build result expression from common_conjunction_expression and inflated_disjunction_remainder:
  //         `(a AND c)` AND `c OR (d AND e)`
  if (common_conjunction_expression && inflated_disjunction_remainder) {
    input_expression = and_(common_conjunction_expression, inflated_disjunction_remainder);
  } else {
    if (common_conjunction_expression) {
      input_expression = common_conjunction_expression;
    } else {
      Assert(inflated_disjunction_remainder,
             "Bug detected. inflated_disjunction_remainder should contain an expression");
      input_expression = inflated_disjunction_remainder;
    }
  }

  return input_expression;
}

const std::shared_ptr<AbstractExpression>& ExpressionReductionRule::reduce_in_with_single_list_element(
    std::shared_ptr<AbstractExpression>& input_expression) {
  if (const auto in_expression = std::dynamic_pointer_cast<InExpression>(input_expression)) {
    if (const auto list_expression = std::dynamic_pointer_cast<ListExpression>(in_expression->set())) {
      if (list_expression->arguments.size() == 1) {
        input_expression = equals_(in_expression->value(), list_expression->arguments[0]);
      }
    }
  }

  for (auto& argument : input_expression->arguments) {
    reduce_in_with_single_list_element(argument);
  }

  return input_expression;
}

void ExpressionReductionRule::reduce_constant_expression(std::shared_ptr<AbstractExpression>& input_expression) {
  for (auto& argument : input_expression->arguments) {
    reduce_constant_expression(argument);
  }

  if (input_expression->arguments.empty()) return;

  // Only prune a whitelisted selection of ExpressionTypes, because we can't, e.g., prune List of literals.
  if (input_expression->type != ExpressionType::Predicate && input_expression->type != ExpressionType::Arithmetic &&
      input_expression->type != ExpressionType::Logical) {
    return;
  }

  const auto all_arguments_are_values =
      std::all_of(input_expression->arguments.begin(), input_expression->arguments.end(),
                  [&](const auto& argument) { return argument->type == ExpressionType::Value; });

  if (!all_arguments_are_values) return;

  resolve_data_type(input_expression->data_type(), [&](const auto data_type_t) {
    using ExpressionDataType = typename decltype(data_type_t)::type;
    const auto result = ExpressionEvaluator{}.evaluate_expression_to_result<ExpressionDataType>(*input_expression);
    Assert(result->is_literal(), "Expected Literal");

    if (result->is_null(0)) {
      input_expression = std::make_shared<ValueExpression>(NullValue{});
    } else {
      input_expression = std::make_shared<ValueExpression>(result->value(0));
    }
  });
}
}  // namespace opossum
