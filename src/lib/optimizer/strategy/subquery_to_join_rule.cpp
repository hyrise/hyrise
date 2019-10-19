#include "subquery_to_join_rule.hpp"

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <utility>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/in_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

/**
 * Calculates which input LQPs of a node are safe to pull predicates from.
 *
 * This is used during the two recursive LQP traversals in the predicate pull-up phase. The first bool is true when we
 * should recurse into the left sub-tree, the right accordingly for the right sub-tree.
 */
std::pair<bool, bool> calculate_safe_recursion_sides(const std::shared_ptr<AbstractLQPNode>& node) {
  switch (node->type) {
    case LQPNodeType::Join: {
      const auto join_node = std::static_pointer_cast<JoinNode>(node);
      // We can safely pull out predicates from any non-null producing side of joins. We also cannot pull up predicates
      // from the right side of semi-/anti-joins, since the columns from that side are not (and cannot be) preserved.
      switch (join_node->join_mode) {
        case JoinMode::Inner:
        case JoinMode::Cross:
          return {true, true};
        case JoinMode::Left:
        case JoinMode::Semi:
        case JoinMode::AntiNullAsFalse:
        case JoinMode::AntiNullAsTrue:
          return {true, false};
        case JoinMode::Right:
          return {false, true};
        case JoinMode::FullOuter:
          return {false, false};
      }
      break;
    }
    case LQPNodeType::Predicate:
    case LQPNodeType::Aggregate:
    case LQPNodeType::Alias:
    case LQPNodeType::Projection:
    case LQPNodeType::Sort:
    case LQPNodeType::Validate:
      return {true, false};
    default:
      return {false, false};
  }
  Fail("Invalid enum value");
}

/**
 * Recursively remove correlated predicate nodes and adapt other nodes as necessary.
 *
 * To handle nodes with more than one output (for example diamond LQPs), we cache the result for such nodes. This
 * avoids adapting the LQP twice but also adding join predicates twice. The boolean in the returned pair indicates
 * whether the result was loaded from cache.
 */
std::pair<SubqueryToJoinRule::PredicatePullUpResult, bool> pull_up_correlated_predicates_recursive(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping,
    std::map<std::shared_ptr<AbstractLQPNode>, SubqueryToJoinRule::PredicatePullUpResult>& result_cache,
    bool is_below_aggregate) {
  const auto cache_it = result_cache.find(node);
  if (cache_it != result_cache.end()) {
    return {cache_it->second, true};
  }

  auto result = SubqueryToJoinRule::PredicatePullUpResult{};
  auto left_input_adapted = node->left_input();
  auto right_input_adapted = node->right_input();
  auto are_inputs_below_aggregate = is_below_aggregate || node->type == LQPNodeType::Aggregate;

  const auto& [should_recurse_left, should_recurse_right] = calculate_safe_recursion_sides(node);
  if (should_recurse_left) {
    const auto& [left_result, left_cached] = pull_up_correlated_predicates_recursive(
        node->left_input(), parameter_mapping, result_cache, are_inputs_below_aggregate);
    left_input_adapted = left_result.adapted_lqp;
    result.required_column_expressions = left_result.required_column_expressions;
    // If the sub-LQP was already visited, its join predicates have already been considered somewhere else and will be
    // part of the final result.
    if (!left_cached) {
      result.join_predicates = left_result.join_predicates;
      result.pulled_predicate_node_count += left_result.pulled_predicate_node_count;
    }
  }
  if (should_recurse_right) {
    const auto& [right_result, right_cached] = pull_up_correlated_predicates_recursive(
        node->right_input(), parameter_mapping, result_cache, are_inputs_below_aggregate);
    right_input_adapted = right_result.adapted_lqp;
    for (const auto& column_expression : right_result.required_column_expressions) {
      const auto find_it = std::find(result.required_column_expressions.cbegin(),
                                     result.required_column_expressions.cend(), column_expression);
      if (find_it == result.required_column_expressions.cend()) {
        result.required_column_expressions.emplace_back(column_expression);
      }
    }
    if (!right_cached) {
      result.join_predicates.insert(result.join_predicates.cend(), right_result.join_predicates.cbegin(),
                                    right_result.join_predicates.cend());
      result.pulled_predicate_node_count += right_result.pulled_predicate_node_count;
    }
  }

  switch (node->type) {
    case LQPNodeType::Predicate: {
      const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
      const auto& [join_predicates, remaining_expression] =
          SubqueryToJoinRule::try_to_extract_join_predicates(predicate_node, parameter_mapping, is_below_aggregate);
      if (remaining_expression) {
        result.adapted_lqp = PredicateNode::make(remaining_expression, left_input_adapted);
      } else {
        result.adapted_lqp = left_input_adapted;
        DebugAssert(!node->right_input(), "Predicate nodes should not have right inputs");
      }
      if (!join_predicates.empty()) {
        result.pulled_predicate_node_count++;
        result.join_predicates.insert(result.join_predicates.end(), join_predicates.begin(), join_predicates.end());
        for (const auto& join_predicate : join_predicates) {
          const auto& column_expression = join_predicate->right_operand();
          auto find_it = std::find(result.required_column_expressions.begin(), result.required_column_expressions.end(),
                                   column_expression);
          if (find_it == result.required_column_expressions.end()) {
            result.required_column_expressions.emplace_back(column_expression);
          }
        }
      }
      break;
    }
    case LQPNodeType::Aggregate:
      result.adapted_lqp = SubqueryToJoinRule::adapt_aggregate_node(std::static_pointer_cast<AggregateNode>(node),
                                                                    result.required_column_expressions);
      result.adapted_lqp->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Alias:
      result.adapted_lqp = SubqueryToJoinRule::adapt_alias_node(std::static_pointer_cast<AliasNode>(node),
                                                                result.required_column_expressions);
      result.adapted_lqp->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Projection:
      result.adapted_lqp = SubqueryToJoinRule::adapt_projection_node(std::static_pointer_cast<ProjectionNode>(node),
                                                                     result.required_column_expressions);
      result.adapted_lqp->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Sort: {
      const auto& sort_node = std::static_pointer_cast<SortNode>(node);
      result.adapted_lqp = SortNode::make(sort_node->node_expressions, sort_node->order_by_modes, left_input_adapted);
      break;
    }
    case LQPNodeType::Validate:
      result.adapted_lqp = ValidateNode::make(left_input_adapted);
      break;
    case LQPNodeType::Join: {
      const auto& join_node = std::static_pointer_cast<JoinNode>(node);
      if (join_node->join_mode == JoinMode::Cross) {
        result.adapted_lqp = JoinNode::make(JoinMode::Cross, left_input_adapted, right_input_adapted);
      } else {
        result.adapted_lqp =
            JoinNode::make(join_node->join_mode, join_node->join_predicates(), left_input_adapted, right_input_adapted);
      }
      break;
    }
    default:
      // Nodes of any other type stop the recursion and thus don't need to be adapted
      DebugAssert(!should_recurse_left && !should_recurse_right,
                  "Nodes that don't stop the recursion need to be adapted/copied");
      result.adapted_lqp = node;
      break;
  }

  if (node->output_count() > 1) {
    result_cache.emplace(node, result);
  }

  return {result, false};
}

void push_arithmetic_expression_into_subquery(const std::shared_ptr<BinaryPredicateExpression>& predicate_expression,
                                              const std::shared_ptr<ArithmeticExpression>& arithmetic_expression,
                                              const std::shared_ptr<LQPSubqueryExpression>& subquery_expression,
                                              size_t arithmetic_expression_argument_idx,
                                              bool subquery_expression_is_left) {
  DebugAssert(
      subquery_expression->lqp->node_expressions.size() == 1,
      "Subqueries used in arithmetic expressions must return a single value, so they must have one node_expression");

  // (1) Create a new arithmetic expression that takes the result expression of the subquery and applies the arithmetic
  //     operation on it.
  const auto new_arithmetic_expression = std::make_shared<ArithmeticExpression>(
      arithmetic_expression->arithmetic_operator,
      subquery_expression_is_left ? subquery_expression->lqp->node_expressions[0]
                                  : arithmetic_expression->left_operand(),
      subquery_expression_is_left ? arithmetic_expression->right_operand()
                                  : subquery_expression->lqp->node_expressions[0]);

  // (2) Insert a ProjectionNode on top of the subquery's LQP that evaluates this arithmetic expression.
  subquery_expression->lqp = ProjectionNode::make(
      std::vector<std::shared_ptr<AbstractExpression>>{new_arithmetic_expression}, subquery_expression->lqp);

  // (3) In the PredicateExpression outside of the LQP, replace the old arithmetic expression with the updated subquery
  //     expression.
  predicate_expression->arguments[arithmetic_expression_argument_idx] = subquery_expression;
}

}  // namespace

namespace opossum {

std::optional<SubqueryToJoinRule::PredicateNodeInfo> SubqueryToJoinRule::is_predicate_node_join_candidate(
    const PredicateNode& predicate_node) {
  PredicateNodeInfo result;

  if (const auto in_expression = std::dynamic_pointer_cast<InExpression>(predicate_node.predicate())) {
    // Only optimize if the set is a subquery and not a static list
    if (in_expression->set()->type != ExpressionType::LQPSubquery) {
      return std::nullopt;
    }

    result.join_mode = in_expression->is_negated() ? JoinMode::AntiNullAsTrue : JoinMode::Semi;
    result.subquery = std::static_pointer_cast<LQPSubqueryExpression>(in_expression->set());
    result.join_predicate = equals_(in_expression->value(), result.subquery->lqp->column_expressions()[0]);

    // Correlated NOT IN is very weird w.r.t. handling of null values and cannot be turned into a
    // multi-predicate join that treats all its predicates equivalently
    if (in_expression->is_negated() && result.subquery->is_correlated()) {
      return std::nullopt;
    }

  } else if (const auto binary_predicate =
                 std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate_node.predicate())) {
    result.join_mode = JoinMode::Semi;

    /**
     * Identify a subquery in an arithmetic expression and push the arithmetics into the subquery.
     * e.g. SELECT * FROM a WHERE a.a > 3 * (SELECT SUM(b.a) FROM b WHERE b.b = a.b)
     * becomes SELECT * FROM a WHERE a.a > (SELECT 3 * SUM(b.a) FROM b WHERE b.b = a.b)
     * We cover 4 cases that are essentially the same: The subquery could be in the left/right operand of the binary
     * predicate and then it could be the left/right operand of that arithmetic expression.
     * More complex cases, such as SELECT * FROM a WHERE a.a > 1 + (SELECT ... ) + 2 are not covered yet.
     */

    if (const auto arithmetic_expression =
            std::dynamic_pointer_cast<ArithmeticExpression>(binary_predicate->left_operand())) {
      if (const auto left_subquery_expression =
              std::dynamic_pointer_cast<LQPSubqueryExpression>(arithmetic_expression->left_operand())) {
        push_arithmetic_expression_into_subquery(binary_predicate, arithmetic_expression, left_subquery_expression, 0,
                                                 true);
      } else if (const auto right_subquery_expression =
                     std::dynamic_pointer_cast<LQPSubqueryExpression>(arithmetic_expression->right_operand())) {
        push_arithmetic_expression_into_subquery(binary_predicate, arithmetic_expression, right_subquery_expression, 0,
                                                 false);
      }
    }

    if (const auto arithmetic_expression =
            std::dynamic_pointer_cast<ArithmeticExpression>(binary_predicate->right_operand())) {
      if (const auto left_subquery_expression =
              std::dynamic_pointer_cast<LQPSubqueryExpression>(arithmetic_expression->left_operand())) {
        push_arithmetic_expression_into_subquery(binary_predicate, arithmetic_expression, left_subquery_expression, 1,
                                                 true);
      } else if (const auto right_subquery_expression =
                     std::dynamic_pointer_cast<LQPSubqueryExpression>(arithmetic_expression->right_operand())) {
        push_arithmetic_expression_into_subquery(binary_predicate, arithmetic_expression, right_subquery_expression, 1,
                                                 false);
      }
    }

    if (const auto left_subquery_expression =
            std::dynamic_pointer_cast<LQPSubqueryExpression>(binary_predicate->left_operand())) {
      result.join_predicate = std::make_shared<BinaryPredicateExpression>(
          flip_predicate_condition(binary_predicate->predicate_condition), binary_predicate->right_operand(),
          left_subquery_expression->lqp->column_expressions()[0]);
      result.subquery = left_subquery_expression;
    } else if (const auto right_subquery_expression =
                   std::dynamic_pointer_cast<LQPSubqueryExpression>(binary_predicate->right_operand())) {
      result.join_predicate = std::make_shared<BinaryPredicateExpression>(
          binary_predicate->predicate_condition, binary_predicate->left_operand(),
          right_subquery_expression->lqp->column_expressions()[0]);
      result.subquery = right_subquery_expression;
    } else {
      return std::nullopt;
    }

  } else if (const auto exists_expression = std::dynamic_pointer_cast<ExistsExpression>(predicate_node.predicate())) {
    result.join_mode = exists_expression->exists_expression_type == ExistsExpressionType::Exists
                           ? JoinMode::Semi
                           : JoinMode::AntiNullAsFalse;
    result.subquery = std::static_pointer_cast<LQPSubqueryExpression>(exists_expression->subquery());

    // We cannot optimize uncorrelated EXISTS into a join
    if (!result.subquery->is_correlated()) {
      return std::nullopt;
    }

  } else {
    return std::nullopt;
  }

  // If the left operand is not a column, e.g. possible for `a + 5 = ...`, then we cannot join on it
  // TODO(anybody) Add the non-column expression as a column (i.e. via a Projection) if this ever comes up
  if (result.join_predicate && !predicate_node.left_input()->find_column_id(*result.join_predicate->left_operand())) {
    return std::nullopt;
  }

  return result;
}

std::pair<bool, size_t> SubqueryToJoinRule::assess_correlated_parameter_usage(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  // Crawl the `lqp`, including subquery-LQPs, for usages of parameters from `parameter_mapping`
  //
  // While counting correlated predicate nodes we don't need to consider diamond LQPs or other forms of nodes occurring
  // twice since visit_lqp only visits each node once. A node might appear again in a subquery (which has its own call
  // to visit_lqp) but in this case the LQP is not optimizable anyway, so we don't care about an accurate count.

  auto optimizable = true;
  auto correlated_predicate_node_count = size_t{0};

  visit_lqp(lqp, [&](const auto& node) {
    if (!optimizable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    auto is_correlated = false;

    for (const auto& expression : node->node_expressions) {
      visit_expression(expression, [&](const auto& sub_expression) {
        if (is_correlated || !optimizable) {
          return ExpressionVisitation::DoNotVisitArguments;
        }

        if (const auto subquery_expression = std::dynamic_pointer_cast<LQPSubqueryExpression>(sub_expression)) {
          const auto& [subquery_optimizable, count_in_subquery] =
              assess_correlated_parameter_usage(subquery_expression->lqp, parameter_mapping);
          optimizable &= subquery_optimizable && count_in_subquery == 0;
        }

        if (const auto parameter_expression =
                std::dynamic_pointer_cast<CorrelatedParameterExpression>(sub_expression)) {
          if (parameter_mapping.find(parameter_expression->parameter_id) != parameter_mapping.end()) {
            is_correlated = true;
          }
        }

        return (is_correlated || !optimizable) ? ExpressionVisitation::DoNotVisitArguments
                                               : ExpressionVisitation::VisitArguments;
      });
    }

    if (is_correlated) {
      if (node->type == LQPNodeType::Predicate) {
        ++correlated_predicate_node_count;
      } else {
        optimizable = false;
      }
    }

    return optimizable ? LQPVisitation::VisitInputs : LQPVisitation::DoNotVisitInputs;
  });

  return {optimizable, correlated_predicate_node_count};
}

std::pair<std::vector<std::shared_ptr<BinaryPredicateExpression>>, std::shared_ptr<AbstractExpression>>
SubqueryToJoinRule::try_to_extract_join_predicates(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping, bool is_below_aggregate) {
  auto join_predicates = std::vector<std::shared_ptr<BinaryPredicateExpression>>{};
  auto anded_expressions = flatten_logical_expressions(predicate_node->predicate(), LogicalOperator::And);

  for (auto& sub_expression : anded_expressions) {
    // Check for the type of expression first. Note that we are not concerned with predicates of other forms using
    // correlated parameters here. We check for parameter usages that prevent optimization in
    // assess_correlated_parameter_usage().
    if (sub_expression->type != ExpressionType::Predicate) {
      continue;
    }

    const auto& predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(sub_expression);

    auto predicate_condition = predicate_expression->predicate_condition;
    if (predicate_condition != PredicateCondition::Equals && predicate_condition != PredicateCondition::NotEquals &&
        predicate_condition != PredicateCondition::GreaterThan &&
        predicate_condition != PredicateCondition::GreaterThanEquals &&
        predicate_condition != PredicateCondition::LessThan &&
        predicate_condition != PredicateCondition::LessThanEquals) {
      continue;
    }

    // We can currently only pull equals predicates above aggregate nodes (by grouping by the column that the predicate
    // compares with). The other predicate types could be supported but would require more sophisticated reformulations.
    if (is_below_aggregate && predicate_condition != PredicateCondition::Equals) {
      continue;
    }

    // Check that one side of the expression is a correlated parameter and the other a column expression of the LQP
    // below the predicate node (required for turning it into a join predicate). Also order the left/right operands by
    // the subplans they originate from.
    const auto& binary_predicate_expression = std::static_pointer_cast<BinaryPredicateExpression>(predicate_expression);
    const auto& left_side = binary_predicate_expression->left_operand();
    const auto& right_side = binary_predicate_expression->right_operand();
    ParameterID parameter_id;
    std::shared_ptr<AbstractExpression> right_operand;
    if (left_side->type == ExpressionType::CorrelatedParameter) {
      parameter_id = std::static_pointer_cast<CorrelatedParameterExpression>(left_side)->parameter_id;
      right_operand = right_side;
    } else if (right_side->type == ExpressionType::CorrelatedParameter) {
      predicate_condition = flip_predicate_condition(predicate_condition);
      parameter_id = std::static_pointer_cast<CorrelatedParameterExpression>(right_side)->parameter_id;
      right_operand = left_side;
    } else {
      continue;
    }

    // We can only use predicates in joins where both operands are columns
    if (!predicate_node->find_column_id(*right_operand)) {
      continue;
    }

    // Is the parameter one we are concerned with? This catches correlated parameters of outer subqueries and
    // placeholders in prepared statements.
    auto expression_it = parameter_mapping.find(parameter_id);
    if (expression_it == parameter_mapping.end()) {
      continue;
    }

    auto left_operand = expression_it->second;
    join_predicates.emplace_back(
        std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand));
    sub_expression = nullptr;
  }

  auto remaining_expression = std::shared_ptr<AbstractExpression>{};
  for (const auto& sub_expression : anded_expressions) {
    if (sub_expression) {
      if (remaining_expression) {
        remaining_expression = and_(remaining_expression, sub_expression);
      } else {
        remaining_expression = sub_expression;
      }
    }
  }

  return {std::move(join_predicates), remaining_expression};
}

std::shared_ptr<AggregateNode> SubqueryToJoinRule::adapt_aggregate_node(
    const std::shared_ptr<AggregateNode>& node,
    const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> group_by_expressions(
      node->node_expressions.cbegin(), node->node_expressions.cbegin() + node->aggregate_expressions_begin_idx);
  ExpressionUnorderedSet original_group_by_expressions(group_by_expressions.cbegin(), group_by_expressions.cend());

  const auto not_found_it = original_group_by_expressions.cend();
  for (const auto& expression : required_column_expressions) {
    if (original_group_by_expressions.find(expression) == not_found_it) {
      group_by_expressions.emplace_back(expression);
    }
  }

  std::vector<std::shared_ptr<AbstractExpression>> aggregate_expressions(
      node->node_expressions.cbegin() + node->aggregate_expressions_begin_idx, node->node_expressions.cend());
  return AggregateNode::make(group_by_expressions, aggregate_expressions);
}

std::shared_ptr<AliasNode> SubqueryToJoinRule::adapt_alias_node(
    const std::shared_ptr<AliasNode>& node,
    const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions) {
  // As with projection nodes, we don't want to add existing columns, but also don't want to deduplicate the existing
  // columns.
  auto expressions = node->node_expressions;
  auto aliases = node->aliases;
  ExpressionUnorderedSet original_expressions(expressions.cbegin(), expressions.cend());

  const auto not_found_it = original_expressions.cend();
  for (const auto& expression : required_column_expressions) {
    if (original_expressions.find(expression) == not_found_it) {
      expressions.emplace_back(expression);
      aliases.emplace_back(expression->as_column_name());
    }
  }

  return AliasNode::make(expressions, aliases);
}

std::shared_ptr<ProjectionNode> SubqueryToJoinRule::adapt_projection_node(
    const std::shared_ptr<ProjectionNode>& node,
    const std::vector<std::shared_ptr<AbstractExpression>>& required_column_expressions) {
  // We don't want to add columns that are already in the projection node. We also don't want to remove duplicates in
  // the expressions of the projection node, so we can't simply build one set containing all expressions
  auto expressions = node->node_expressions;
  ExpressionUnorderedSet original_expressions(expressions.cbegin(), expressions.cend());

  const auto not_found_it = original_expressions.cend();
  for (const auto& expression : required_column_expressions) {
    if (original_expressions.find(expression) == not_found_it) {
      expressions.emplace_back(expression);
    }
  }

  return ProjectionNode::make(expressions);
}

SubqueryToJoinRule::PredicatePullUpResult SubqueryToJoinRule::pull_up_correlated_predicates(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  auto result_cache = std::map<std::shared_ptr<AbstractLQPNode>, SubqueryToJoinRule::PredicatePullUpResult>{};
  return pull_up_correlated_predicates_recursive(node, parameter_mapping, result_cache, false).first;
}

void SubqueryToJoinRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Check if `node` is a PredicateNode with a subquery and try to turn it into an anti- or semi-join.
  // To do this, we
  //   - Check whether node is of a supported type:
  //       - (NOT) IN predicate with a subquery as the right operand
  //       - (NOT) EXISTS predicate
  //       - comparison (<,>,<=,>=,=,<>) predicate with subquery as the right operand
  //   - If node is a (NOT) IN or a comparison, extract a base join predicate
  //   - Scan the subquery-LQP for all usages of correlated parameters, counting the number of predicate nodes using
  //     them
  //     (if one is used outside of predicate nodes, we never optimize the LQP).
  //   - Scan the subquery-LQP for correlated predicates that we need to pull up, and turn each into a join predicate
  //   - Check whether all correlated predicates can be pulled up (abort if not)
  //   - Copy and adapt the subquery-LQP, removing all correlated predicate nodes and adapt nodes above them in the LQP,
  //     so that all columns required by the new join predicates are available at the top of the adapted subquery-LQP.
  //   - Build a join with the collected predicates
  //
  // We always reformulate when possible, since benchmarks have shown that this reformulation makes the execution
  // faster regardless of the expected table sizes, etc.

  /**
   * 1. Skip non-PredicateNodes
   */
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  if (!predicate_node) {
    _apply_to_inputs(node);
    return;
  }

  /**
   * 2. Assess whether the PredicateNode has the general form of one that this rule can turn into a Join.
   *    I.e., `x (NOT) IN (<subquery>)`, `(NOT) EXISTS(<subquery>)` or `x <op> <subquery>`
   */
  auto predicate_node_info = is_predicate_node_join_candidate(*predicate_node);
  if (!predicate_node_info) {
    _apply_to_inputs(node);
    return;
  }

  /**
   * 3. Count the number of PredicateNodes in the Subquery-LQP using correlated parameters. Abort if any
   *    non-PredicateNodes or nested subquery-LQPs use correlated parameters.
   */
  std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_mapping;
  for (size_t parameter_idx = 0; parameter_idx < predicate_node_info->subquery->parameter_count(); ++parameter_idx) {
    const auto& parameter_expression = predicate_node_info->subquery->parameter_expression(parameter_idx);
    parameter_mapping.emplace(predicate_node_info->subquery->parameter_ids[parameter_idx], parameter_expression);
  }

  const auto& [optimizable, correlated_predicate_node_count] =
      assess_correlated_parameter_usage(predicate_node_info->subquery->lqp, parameter_mapping);
  if (!optimizable) {
    _apply_to_inputs(node);
    return;
  }

  /**
   *
   */
  const auto pull_up_result = pull_up_correlated_predicates(predicate_node_info->subquery->lqp, parameter_mapping);
  if (pull_up_result.pulled_predicate_node_count != correlated_predicate_node_count) {
    // Not all correlated predicate nodes can be pulled up
    DebugAssert(pull_up_result.pulled_predicate_node_count < correlated_predicate_node_count,
                "Inconsistent results from scan for correlated predicate nodes");
    _apply_to_inputs(node);
    return;
  }

  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>();
  join_predicates.reserve(pull_up_result.join_predicates.size() + (predicate_node_info->join_predicate ? 1 : 0));
  if (predicate_node_info->join_predicate) {
    join_predicates.emplace_back(predicate_node_info->join_predicate);
  }
  for (const auto& join_predicate : pull_up_result.join_predicates) {
    join_predicates.emplace_back(join_predicate);
  }

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. Check that one exists, but rely on join predicate ordering rule to move it to the front.
  if (std::find_if(join_predicates.begin(), join_predicates.end(),
                   [](const std::shared_ptr<AbstractExpression>& expression) {
                     return std::static_pointer_cast<AbstractPredicateExpression>(expression)->predicate_condition ==
                            PredicateCondition::Equals;
                   }) == join_predicates.end()) {
    _apply_to_inputs(node);
    return;
  }

  const auto join_mode = predicate_node_info->join_mode;
  const auto join_node = JoinNode::make(join_mode, join_predicates);
  lqp_replace_node(node, join_node);
  join_node->set_right_input(pull_up_result.adapted_lqp);

  if (pull_up_result.adapted_lqp) {
    const auto& estimator = cost_estimator->cardinality_estimator;
    const auto probe_side_cardinality = estimator->estimate_cardinality(join_node->left_input());
    const auto build_side_cardinality = estimator->estimate_cardinality(join_node->right_input());
    const auto& primary_join_predicate = join_predicates[0];

    bool primary_join_predicate_is_equals = false;
    if (const auto predicate_expression =
            std::dynamic_pointer_cast<BinaryPredicateExpression>(primary_join_predicate)) {
      if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
        primary_join_predicate_is_equals = true;
      }
    }

    if ((join_mode == JoinMode::Semi || join_mode == JoinMode::AntiNullAsTrue ||
         join_mode == JoinMode::AntiNullAsFalse) &&
        build_side_cardinality > probe_side_cardinality * 10 && build_side_cardinality > 1'000 &&
        primary_join_predicate_is_equals) {
      // Semi/Anti joins are currently handled by the hash join, which performs badly if the right side is much bigger
      // than the left side. For that case, we add a second semi join on the build side, which throws out all values
      // that will not be found by the primary (first) predicate of the later join, anyway. This is the case no matter
      // if the original join is a semi or anti join. In any case, we want the reducing join introduced here to limit
      // the input to the join operator to those values that have a chance of being relevant for the semi/anti join.
      // However, we can only throw away values on the build side if the primary predicate is an equals predicate. For
      // an example, see TPC-H query 21. That query is the main reason this part exists. The current thresholds are
      // somewhat arbitrary. If we start to see more of those cases, we can try and find a more complex heuristic. This
      // is also called "semi join reduction": http://www.db.in.tum.de/research/publications/conferences/semijoin.pdf
      const auto pre_join_node = JoinNode::make(JoinMode::Semi, primary_join_predicate);
      pre_join_node->comment = "Semi Reduction";
      lqp_insert_node(join_node, LQPInputSide::Right, pre_join_node);
      pre_join_node->set_right_input(join_node->left_input());
    }
  }

  _apply_to_inputs(join_node);
}

}  // namespace opossum
