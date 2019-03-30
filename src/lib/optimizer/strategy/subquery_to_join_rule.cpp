#include "subquery_to_join_rule.hpp"

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <utility>

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
  Fail("GCC thinks this is reachable");
}

void find_pullable_predicate_nodes_recursive(
    const std::shared_ptr<AbstractLQPNode>& node,
    std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<BinaryPredicateExpression>>>&
        pullable_predicate_nodes,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping, bool is_below_aggregate) {
  if (node->type == LQPNodeType::Predicate) {
    const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
    auto join_predicate =
        SubqueryToJoinRule::try_to_extract_join_predicate(predicate_node, parameter_mapping, is_below_aggregate);
    if (join_predicate) {
      pullable_predicate_nodes.emplace_back(predicate_node, std::move(join_predicate));
    }
  } else if (node->type == LQPNodeType::Aggregate) {
    is_below_aggregate = true;
  }

  const auto& [should_recurse_left, should_recurse_right] = calculate_safe_recursion_sides(node);
  if (should_recurse_left) {
    DebugAssert(node->left_input(), "Nodes of this type should always have a left input");
    find_pullable_predicate_nodes_recursive(node->left_input(), pullable_predicate_nodes, parameter_mapping,
                                            is_below_aggregate);
  }
  if (should_recurse_right) {
    DebugAssert(node->right_input(), "Nodes of this type should always have a right input");
    find_pullable_predicate_nodes_recursive(node->right_input(), pullable_predicate_nodes, parameter_mapping,
                                            is_below_aggregate);
  }
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
  /**
   * Crawl the `lqp`, including subquery-LQPs, for usages of parameters from `parameter_mapping`
   */

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
          const auto& [_, count_in_subquery] =
              assess_correlated_parameter_usage(subquery_expression->lqp, parameter_mapping);
          if (count_in_subquery > 0) {
            correlated_predicate_node_count += count_in_subquery;
            optimizable = false;
          }
        }

        if (const auto parameter_expression =
                std::dynamic_pointer_cast<CorrelatedParameterExpression>(sub_expression)) {
          if (parameter_mapping.find(parameter_expression->parameter_id) != parameter_mapping.end()) {
            is_correlated = true;
          }
        }

        return is_correlated ? ExpressionVisitation::DoNotVisitArguments : ExpressionVisitation::VisitArguments;
      });
    }

    if (is_correlated) {
      if (node->type == LQPNodeType::Predicate) {
        ++correlated_predicate_node_count;
      } else {
        optimizable = false;
        return LQPVisitation::DoNotVisitInputs;
      }
    }

    return LQPVisitation::VisitInputs;
  });

  return {!optimizable, correlated_predicate_node_count};
}

std::shared_ptr<BinaryPredicateExpression> SubqueryToJoinRule::try_to_extract_join_predicate(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping, bool is_below_aggregate) {
  // Check for the type of expression first. Note that we are not concerned with predicates of other forms using
  // correlated parameters here. We check for parameter usages that prevent optimization later in
  // assess_correlated_parameter_usage().
  if (predicate_node->predicate()->type != ExpressionType::Predicate) {
    return nullptr;
  }

  const auto& predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());

  // We rely on PredicateSplitUpRule having split up ANDed chains of such predicates previously, so that we can process
  // them separately.
  auto predicate_condition = predicate_expression->predicate_condition;
  if (!is_binary_predicate_condition(predicate_condition)) {
    return nullptr;
  }

  // We can currently only pull equals predicates above aggregate nodes (by grouping by the column that the predicate
  // compares with). The other predicate types could be supported but would require more sophisticated reformulations.
  if (is_below_aggregate && predicate_condition != PredicateCondition::Equals) {
    return nullptr;
  }

  // Check that one side of the expression is a correlated parameter and the other a column expression of the LQP below
  // the predicate node (required for turning it into a join predicate). Also order the left/right operands by the
  // subtrees they originate from.
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
    return nullptr;
  }

  // We can only use predicates in joins where both operands are columns
  if (!predicate_node->find_column_id(*right_operand)) {
    return nullptr;
  }

  // Is the parameter one we are concerned with? This catches correlated parameters of outer subqueries and
  // placeholders in prepared statements.
  auto expression_it = parameter_mapping.find(parameter_id);
  if (expression_it == parameter_mapping.end()) {
    return nullptr;
  }

  auto left_operand = expression_it->second;
  return std::make_shared<BinaryPredicateExpression>(predicate_condition, left_operand, right_operand);
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

std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<BinaryPredicateExpression>>>
SubqueryToJoinRule::find_pullable_predicate_nodes(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<BinaryPredicateExpression>>>
      pullable_predicate_nodes;
  find_pullable_predicate_nodes_recursive(node, pullable_predicate_nodes, parameter_mapping, false);
  return pullable_predicate_nodes;
}

SubqueryToJoinRule::PredicatePullUpInfo SubqueryToJoinRule::copy_and_adapt_lqp(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<BinaryPredicateExpression>>>&
        pullable_predicate_nodes) {
  // Recursively traverse the subquery LQP, remove correlated predicate nodes and adapt other nodes as needed. Since
  // how we need to adapt nodes depends on the correlated predicate nodes removed below them, we recurse first and keep
  // of the column expressions required by the removed predicate nodes.
  // We copy every node above a correlated predicate, so that if a node has multiple outputs the other outputs still
  // reference the unchanged node and thus don't change semantically.
  const auto& [should_recurse_left, should_recurse_right] = calculate_safe_recursion_sides(node);
  auto left_input_adapted = node->left_input();
  auto right_input_adapted = node->right_input();
  std::vector<std::shared_ptr<AbstractExpression>> required_column_expressions;
  if (should_recurse_left) {
    DebugAssert(node->left_input(), "Nodes of this type should always have a left input");
    auto left_info = copy_and_adapt_lqp(node->left_input(), pullable_predicate_nodes);
    left_input_adapted = left_info.adapted_lqp;
    required_column_expressions = std::move(left_info.required_column_expressions);
  }
  if (should_recurse_right) {
    DebugAssert(node->right_input(), "Nodes of this type should always have a right input");
    auto right_info = copy_and_adapt_lqp(node->right_input(), pullable_predicate_nodes);
    right_input_adapted = right_info.adapted_lqp;
    required_column_expressions.insert(required_column_expressions.end(),
                                       right_info.required_column_expressions.begin(),
                                       right_info.required_column_expressions.end());
  }

  std::shared_ptr<AbstractLQPNode> adapted_node;
  switch (node->type) {
    case LQPNodeType::Predicate: {
      const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
      const auto pair_it = std::find_if(pullable_predicate_nodes.begin(), pullable_predicate_nodes.end(),
                                        [&](const auto& pair) { return pair.first == predicate_node; });
      if (pair_it == pullable_predicate_nodes.end()) {
        // Uncorrelated predicate node, needs to be copied
        adapted_node = PredicateNode::make(predicate_node->predicate(), left_input_adapted);
      } else {
        // Correlated predicate node, needs to be removed
        adapted_node = left_input_adapted;
        const auto& column_expression = pair_it->second->right_operand();
        if (std::find(required_column_expressions.begin(), required_column_expressions.end(), column_expression) ==
            required_column_expressions.end()) {
          required_column_expressions.emplace_back(column_expression);
        }
      }
      break;
    }
    case LQPNodeType::Aggregate:
      adapted_node = adapt_aggregate_node(std::static_pointer_cast<AggregateNode>(node), required_column_expressions);
      adapted_node->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Alias:
      adapted_node = adapt_alias_node(std::static_pointer_cast<AliasNode>(node), required_column_expressions);
      adapted_node->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Projection:
      adapted_node = adapt_projection_node(std::static_pointer_cast<ProjectionNode>(node), required_column_expressions);
      adapted_node->set_left_input(left_input_adapted);
      break;
    case LQPNodeType::Sort: {
      const auto& sort_node = std::static_pointer_cast<SortNode>(node);
      adapted_node = SortNode::make(sort_node->node_expressions, sort_node->order_by_modes, left_input_adapted);
      break;
    }
    case LQPNodeType::Validate:
      adapted_node = ValidateNode::make(left_input_adapted);
      break;
    case LQPNodeType::Join: {
      const auto& join_node = std::static_pointer_cast<JoinNode>(node);
      if (join_node->join_mode == JoinMode::Cross) {
        adapted_node = JoinNode::make(JoinMode::Cross, left_input_adapted, right_input_adapted);
      } else {
        adapted_node =
            JoinNode::make(join_node->join_mode, join_node->join_predicates(), left_input_adapted, right_input_adapted);
      }
      break;
    }
    default:
      // Nodes of any other type stop the recursion and thus don't need to be adapted
      DebugAssert(!should_recurse_left && !should_recurse_right,
                  "Nodes that don't stop the recursion need to be adapted/copied") adapted_node = node;
      break;
  }

  return {adapted_node, std::move(required_column_expressions)};
}

std::string SubqueryToJoinRule::name() const { return "Subquery to Join Rule"; }

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

  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);

  if (!predicate_node) {
    _apply_to_inputs(node);
    return;
  }

  /**
   * Assess whether the PredicateNode has the general form of one that this rule can turn into a Join.
   * I.e., `x (NOT) IN (<subquery>)`, `(NOT) EXISTS(<subquery>)` or `x <op> <subquery>`
   */
  auto predicate_node_info = is_predicate_node_join_candidate(*predicate_node);
  if (!predicate_node_info) {
    _apply_to_inputs(node);
    return;
  }

  std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_mapping;
  for (size_t parameter_idx = 0; parameter_idx < predicate_node_info->subquery->parameter_count(); ++parameter_idx) {
    const auto& parameter_expression = predicate_node_info->subquery->parameter_expression(parameter_idx);
    parameter_mapping.emplace(predicate_node_info->subquery->parameter_ids[parameter_idx], parameter_expression);
  }

  const auto& [not_optimizable, correlated_predicate_node_count] =
      assess_correlated_parameter_usage(predicate_node_info->subquery->lqp, parameter_mapping);
  if (not_optimizable) {
    _apply_to_inputs(node);
    return;
  }

  const auto pullable_predicate_nodes =
      find_pullable_predicate_nodes(predicate_node_info->subquery->lqp, parameter_mapping);
  if (pullable_predicate_nodes.size() != correlated_predicate_node_count) {
    // Not all correlated predicate nodes can be pulled up
    DebugAssert(pullable_predicate_nodes.size() < correlated_predicate_node_count,
                "Inconsistent results from scan for correlated predicate nodes");
    _apply_to_inputs(node);
    return;
  }

  auto pull_up_info = copy_and_adapt_lqp(predicate_node_info->subquery->lqp, pullable_predicate_nodes);

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. Check that one exists and move it to the front.
  auto join_predicates = std::vector<std::shared_ptr<AbstractExpression>>();
  join_predicates.reserve(pullable_predicate_nodes.size() + (predicate_node_info->join_predicate ? 1 : 0));
  auto found_equals_predicate = false;
  if (predicate_node_info->join_predicate) {
    join_predicates.emplace_back(predicate_node_info->join_predicate);
    found_equals_predicate = predicate_node_info->join_predicate->predicate_condition == PredicateCondition::Equals;
  }
  for (const auto& [_, join_predicate] : pullable_predicate_nodes) {
    join_predicates.emplace_back(join_predicate);
    if (!found_equals_predicate && join_predicate->predicate_condition == PredicateCondition::Equals) {
      std::swap(join_predicates.front(), join_predicates.back());
      found_equals_predicate = true;
    }
  }

  if (join_predicates.empty() || !found_equals_predicate) {
    _apply_to_inputs(node);
    return;
  }

  const auto join_node = JoinNode::make(predicate_node_info->join_mode, join_predicates);
  lqp_replace_node(node, join_node);
  join_node->set_right_input(pull_up_info.adapted_lqp);

  _apply_to_inputs(join_node);
}

}  // namespace opossum
