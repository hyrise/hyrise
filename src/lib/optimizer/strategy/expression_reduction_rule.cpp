#include "expression_reduction_rule.hpp"

#include <algorithm>
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

void ExpressionReductionRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  Assert(node->type == LQPNodeType::Root, "ExpressionReductionRule needs root to hold onto");

  visit_lqp(node, [&](const auto& sub_node) {
    if (sub_node->type == LQPNodeType::Aggregate) {
      remove_duplicate_aggregate(sub_node->node_expressions, sub_node, node);
    }

    for (auto& expression : sub_node->node_expressions) {
      reduce_distributivity(expression);
      rewrite_like_prefix_wildcard(expression);

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

void ExpressionReductionRule::rewrite_like_prefix_wildcard(std::shared_ptr<AbstractExpression>& input_expression) {
  // Continue only if the expression is a LIKE/NOT LIKE expression
  const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(input_expression);
  if (!binary_predicate) {
    return;
  }
  if (binary_predicate->predicate_condition != PredicateCondition::Like &&
      binary_predicate->predicate_condition != PredicateCondition::NotLike) {
    return;
  }

  // Continue only if right operand is a literal/value (expr LIKE 'asdf%')
  const auto pattern_value_expression = std::dynamic_pointer_cast<ValueExpression>(binary_predicate->right_operand());
  if (!pattern_value_expression) {
    return;
  }

  const auto pattern = boost::get<pmr_string>(pattern_value_expression->value);

  // Continue only if the pattern ends with a "%"-wildcard, has a non-empty prefix and contains no other wildcards
  const auto single_char_wildcard_pos = pattern.find_first_of('_');

  if (single_char_wildcard_pos != pmr_string::npos) {
    return;
  }

  const auto multi_char_wildcard_pos = pattern.find_first_of('%');
  if (multi_char_wildcard_pos == std::string::npos || multi_char_wildcard_pos == 0 ||
      multi_char_wildcard_pos + 1 != pattern.size()) {
    return;
  }

  // Calculate lower and upper bound of the search pattern
  const auto lower_bound = pattern.substr(0, multi_char_wildcard_pos);
  const auto current_character_value = static_cast<int>(lower_bound.back());

  // Find next value according to ASCII-table
  constexpr int MAX_ASCII_VALUE = 127;
  if (current_character_value >= MAX_ASCII_VALUE) {
    return;
  }

  const auto next_character = static_cast<char>(current_character_value + 1);
  const auto upper_bound = lower_bound.substr(0, lower_bound.size() - 1) + next_character;

  if (binary_predicate->predicate_condition == PredicateCondition::Like) {
    input_expression = between_upper_exclusive_(binary_predicate->left_operand(), lower_bound, upper_bound);
  } else {  // binary_predicate->predicate_condition == PredicateCondition::NotLike
    input_expression = or_(less_than_(binary_predicate->left_operand(), lower_bound),
                           greater_than_equals_(binary_predicate->left_operand(), upper_bound));
  }
}

void ExpressionReductionRule::remove_duplicate_aggregate(
    std::vector<std::shared_ptr<AbstractExpression>>& input_expressions,
    const std::shared_ptr<AbstractLQPNode>& aggregate_node, const std::shared_ptr<AbstractLQPNode>& root_node) {
  // Create a list of all sums, counts, and averages in the aggregate node.
  std::vector<std::reference_wrapper<const std::shared_ptr<AbstractExpression>>> sums, counts, avgs;
  for (auto& input_expression : input_expressions) {
    if (input_expression->type != ExpressionType::Aggregate) continue;
    auto& aggregate_expression = static_cast<AggregateExpression&>(*input_expression);
    switch (aggregate_expression.aggregate_function) {
      case AggregateFunction::Sum: {
        sums.emplace_back(input_expression);
        break;
      }
      case AggregateFunction::Count: {
        counts.emplace_back(input_expression);
        break;
      }
      case AggregateFunction::Avg: {
        avgs.emplace_back(input_expression);
        break;
      }
      default:
        continue;
    }
  }

  // Take a copy of what the aggregate was originally supposed to do
  const auto original_aggregate_expressions = aggregate_node->column_expressions();

  const auto& aggregate_input_node = aggregate_node->left_input();
  auto replacements = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>{};

  // Iterate over the AVGs, check if matching SUMs and COUNTs exist, and add a suitable replacement to `replacements`.
  for (const auto& avg_expression_ptr : avgs) {
    const auto& avg_expression = static_cast<AggregateExpression&>(*avg_expression_ptr.get());

    const auto& avg_argument = avg_expression.argument();
    const auto avg_argument_is_nullable = avg_argument.get()->is_nullable_on_lqp(*aggregate_input_node);

    // A helper function that checks whether SUMs and COUNTs match the AVG
    const auto finder = [&](const auto& other_expression) {
      const auto other_argument = static_cast<const AggregateExpression&>(*other_expression.get().get()).argument();

      if (!other_argument) {
        // other_argument might be nullptr if we are looking at COUNT(*) - that is acceptable if the argument a in
        // AVG(a) is not nullable. In that case, COUNT(*) == COUNT(a).
        return !avg_argument_is_nullable;
      }

      return other_argument == avg_argument || (other_argument && *other_argument == *avg_argument);
    };

    auto sum_it = std::find_if(sums.begin(), sums.end(), finder);
    auto count_it = std::find_if(counts.begin(), counts.end(), finder);
    if (sum_it != sums.end() && count_it != counts.end()) {
      // Found matching SUM and COUNT (either COUNT(a) or COUNT(*) for a non-NULL a) - add it to the replacements list
      // The cast will become unnecessary once #1799 is fixed
      replacements[avg_expression_ptr] = div_(cast_(sum_it->get(), DataType::Double), count_it->get());
    }
  }

  // No replacements possible
  if (replacements.empty()) return;

  // Back up the current column names
  const auto& root_expressions = root_node->column_expressions();
  auto old_column_names = std::vector<std::string>(root_expressions.size());
  for (auto expression_idx = size_t{0}; expression_idx < root_expressions.size(); ++expression_idx) {
    old_column_names[expression_idx] = root_expressions[expression_idx]->as_column_name();
  }

  {
    // Remove the AVG() expression from the AggregateNode
    auto& expressions = aggregate_node->node_expressions;
    expressions.erase(
        std::remove_if(expressions.begin(), expressions.end(),
                       [&](const auto& expression) { return replacements.find(expression) != replacements.end(); }),
        expressions.end());
  }

  // Add a ProjectionNode that calculates AVG(a) as SUM(a)/COUNT(a).
  const auto projection_node = std::make_shared<ProjectionNode>(original_aggregate_expressions);
  lqp_replace_node(aggregate_node, projection_node);
  lqp_insert_node(projection_node, LQPInputSide::Left, aggregate_node);

  // Now update the AVG expression in all nodes that might refer to it, starting with the ProjectionNode
  bool updated_an_alias = false;
  visit_lqp_upwards(projection_node, [&](const auto& node) {
    for (auto& expression : node->node_expressions) {
      expression_deep_replace(expression, replacements);
    }

    if (node->type == LQPNodeType::Alias) updated_an_alias = true;

    return LQPUpwardVisitation::VisitOutputs;
  });

  // If there is no upward AliasNode, we need to add one that renames "SUM/COUNT" to "AVG"
  if (!updated_an_alias) {
    auto root_expressions_replaced = root_node->column_expressions();

    for (auto& expression : root_expressions_replaced) {
      expression_deep_replace(expression, replacements);
    }

    const auto alias_node = AliasNode::make(root_expressions_replaced, old_column_names);
    lqp_insert_node(root_node, LQPInputSide::Left, alias_node);
  }
}

}  // namespace opossum
