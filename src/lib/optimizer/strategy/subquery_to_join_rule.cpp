#include "subquery_to_join_rule.hpp"

#include <algorithm>
#include <iterator>
#include <map>
#include <memory>
#include <set>
#include <utility>

#include "cost_model/cost_model_logical.hpp"
#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
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
#include "logical_query_plan/validate_node.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {

/**
 * Used to abstract over the different types of input LQPs handled by this rule.
 */
struct InputLQPInfo {
  /**
   * The subquery expression to optimize.
   */
  const LQPSubqueryExpression& subquery_expression;

  /**
   * Optional additional join predicate for the created semi/anti join.
   */
  std::shared_ptr<BinaryPredicateExpression> additional_join_predicate;

  /**
   * The mode for the created join.
   */
  JoinMode join_mode;
};

/**
 * Used to track information in the predicate pull-up phase.
 */
struct PredicatePullUpInfo {
  /**
   * Root of the adapted LQP below this point.
   */
  std::shared_ptr<AbstractLQPNode> adapted_lqp;

  /**
   * Join predicates extracted from removed correlated predicate nodes.
   *
   * These are constructed so that `left_operand()` is always the column in the left subtree and `right_operand()`
   * the column in the subquery.
   */
  std::vector<std::shared_ptr<BinaryPredicateExpression>> extracted_join_predicates;

  /**
   * Column expressions from the subquery required by the extracted join predicates.
   */
  ExpressionUnorderedSet required_column_expressions;
};

/**
 * Extract information about the input LQP into a general format.
 *
 * Returns nullopt if the LQP does not match one of the supported formats.
 */
std::optional<InputLQPInfo> extract_input_lqp_info(const std::shared_ptr<AbstractLQPNode>& node) {
  if (node->type != LQPNodeType::Predicate) {
    return std::nullopt;
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();
  const auto& left_tree_root = node->left_input();

  switch (predicate_node_predicate->type) {
    case ExpressionType::Predicate: {
      const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node_predicate);

      std::shared_ptr<AbstractExpression> comparison_expression;
      PredicateCondition comparison_condition;
      JoinMode join_mode;
      std::shared_ptr<LQPSubqueryExpression> subquery_expression;
      switch (predicate_expression->predicate_condition) {
        case PredicateCondition::In:
        case PredicateCondition::NotIn: {
          const auto in_expression = std::static_pointer_cast<InExpression>(predicate_expression);
          // Only optimize if the set is a subquery and not a static list
          if (in_expression->set()->type != ExpressionType::LQPSubquery) {
            return std::nullopt;
          }

          subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(in_expression->set());
          if (predicate_expression->predicate_condition == PredicateCondition::NotIn &&
              subquery_expression->is_correlated()) {
            // Correlated NOT IN is very weird w.r.t. handling of null values and cannot be turned into a
            // multi-predicate join that treats all its predicates equivalently
            return std::nullopt;
          }

          comparison_expression = in_expression->value();
          comparison_condition = PredicateCondition::Equals;
          join_mode = in_expression->is_negated() ? JoinMode::AntiDiscardNulls : JoinMode::Semi;
          break;
        }
        case PredicateCondition::Equals:
        case PredicateCondition::NotEquals:
        case PredicateCondition::LessThan:
        case PredicateCondition::LessThanEquals:
        case PredicateCondition::GreaterThan:
        case PredicateCondition::GreaterThanEquals: {
          const auto& binary_predicate_expression =
              std::static_pointer_cast<BinaryPredicateExpression>(predicate_expression);
          join_mode = JoinMode::Semi;
          comparison_condition = binary_predicate_expression->predicate_condition;

          if (binary_predicate_expression->left_operand()->type == ExpressionType::LQPSubquery) {
            comparison_condition = flip_predicate_condition(comparison_condition);
            subquery_expression =
                std::static_pointer_cast<LQPSubqueryExpression>(binary_predicate_expression->left_operand());
            comparison_expression = binary_predicate_expression->right_operand();
          } else if (binary_predicate_expression->right_operand()->type == ExpressionType::LQPSubquery) {
            subquery_expression =
                std::static_pointer_cast<LQPSubqueryExpression>(binary_predicate_expression->right_operand());
            comparison_expression = binary_predicate_expression->left_operand();
          } else {
            return std::nullopt;
          }

          break;
        }
        default:
          return std::nullopt;
      }

      // Check that the comparison expression is a column expression of the left input tree so that it can be turned
      // into a join predicate.
      if (!left_tree_root->find_column_id(*comparison_expression)) {
        return std::nullopt;
      }

      // Check that the subquery returns a single column, and build a join predicate with it.
      const auto& right_column_expressions = subquery_expression->lqp->column_expressions();
      Assert(right_column_expressions.size() == 1, "IN/comparison subquery should only return a single column");
      auto additional_join_predicate = std::make_shared<BinaryPredicateExpression>(
          comparison_condition, comparison_expression, right_column_expressions.front());
      return InputLQPInfo{*subquery_expression, additional_join_predicate, join_mode};
    }
    case ExpressionType::Exists: {
      const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node_predicate);
      auto exists_subquery = exists_expression->subquery();

      Assert(exists_subquery->type == ExpressionType::LQPSubquery,
             "Optimization rule should be run before LQP translation");
      auto subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(exists_subquery);

      // We cannot optimize uncorrelated exists into a join
      if (!subquery_expression->is_correlated()) {
        return std::nullopt;
      }

      auto join_mode = exists_expression->exists_expression_type == ExistsExpressionType::Exists
                           ? JoinMode::Semi
                           : JoinMode::AntiRetainNulls;
      return InputLQPInfo{*subquery_expression, nullptr, join_mode};
      break;
    }
    default:
      return std::nullopt;
  }
}

/**
 * Check whether an LQP node uses a correlated parameter.
 */
bool uses_correlated_parameters(const std::shared_ptr<AbstractLQPNode>& node,
                                const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  for (const auto& expression : node->node_expressions) {
    bool is_correlated = false;
    visit_expression(expression, [&](const auto& sub_expression) {
      // We already know that the node is correlated, so we can skip the rest of the expression
      if (is_correlated) {
        return ExpressionVisitation::DoNotVisitArguments;
      }

      if (sub_expression->type == ExpressionType::CorrelatedParameter) {
        const auto& parameter_expression = std::static_pointer_cast<CorrelatedParameterExpression>(sub_expression);
        if (parameter_mapping.find(parameter_expression->parameter_id) != parameter_mapping.end()) {
          is_correlated = true;
        }
      }

      return is_correlated ? ExpressionVisitation::DoNotVisitArguments : ExpressionVisitation::VisitArguments;
    });

    if (is_correlated) {
      return true;
    }
  }

  return false;
}

/**
 * Searches for usages of correlated parameters.
 *
 * The first boolean is true when a correlated parameter is used outside of predicate nodes (for example in joins). In
 * this case we can never optimize this LQP. If it is false, the size_t contains the number of predicate nodes in the
 * LQP that use correlated parameters.
 */
std::pair<bool, size_t> assess_correlated_parameter_usage(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  bool optimizable = true;
  size_t correlated_predicate_node_count = 0;
  visit_lqp(lqp, [&](const auto& node) {
    if (!optimizable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (uses_correlated_parameters(node, parameter_mapping)) {
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

/**
 * Tries to safely extract a join predicate from a correlated predicate node.
 *
 * Returns a binary predicate expression where the left operand is always the expression associated with the correlated
 * parameter (and thus a column from the left subtree) and the right operand a column from the subqueries LQP.
 */
std::shared_ptr<BinaryPredicateExpression> try_to_extract_join_predicate(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping, bool is_below_aggregate) {
  // Check for the type of expression first. Note that we are not concerned with predicates of other forms using
  // correlated parameters here. We check for parameter usages that prevent optimization later in
  // contains_unoptimizable_correlated_parameter_usages.
  if (predicate_node->predicate()->type != ExpressionType::Predicate) {
    return nullptr;
  }

  const auto& predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());

  // Joins only support these six binary predicates. We rely on PredicateSplitUpRule having split up ANDed chains of
  // such predicates previously, so that we can process them separately.
  auto predicate_condition = predicate_expression->predicate_condition;
  switch (predicate_condition) {
    case PredicateCondition::Equals:
    case PredicateCondition::NotEquals:
    case PredicateCondition::LessThan:
    case PredicateCondition::LessThanEquals:
    case PredicateCondition::GreaterThan:
    case PredicateCondition::GreaterThanEquals:
      break;
    default:
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
  auto ordered_predicate = binary_predicate_expression->deep_copy();
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

/**
 * Copy an aggregate node and adapt it to group by all required columns.
 */
std::shared_ptr<AggregateNode> adapt_aggregate_node(const AggregateNode& node,
                                                    const ExpressionUnorderedSet& required_column_expressions) {
  std::vector<std::shared_ptr<AbstractExpression>> group_by_expressions(
      node.node_expressions.cbegin(), node.node_expressions.cbegin() + node.aggregate_expressions_begin_idx);
  ExpressionUnorderedSet original_group_by_expressions(group_by_expressions.cbegin(), group_by_expressions.cend());

  const auto not_found_it = original_group_by_expressions.cend();
  for (const auto& expression : required_column_expressions) {
    if (original_group_by_expressions.find(expression) == not_found_it) {
      group_by_expressions.emplace_back(expression);
    }
  }

  std::vector<std::shared_ptr<AbstractExpression>> aggregate_expressions(
      node.node_expressions.cbegin() + node.aggregate_expressions_begin_idx, node.node_expressions.cend());
  return AggregateNode::make(group_by_expressions, aggregate_expressions);
}

/**
 * Copy an alias node and adapt it to keep all required columns.
 */
std::shared_ptr<AliasNode> adapt_alias_node(const AliasNode& node,
                                            const ExpressionUnorderedSet& required_column_expressions) {
  // As with projection nodes, we don't want to add existing columns, but also don't want to deduplicate the existing
  // columns.
  auto expressions = node.node_expressions;
  auto aliases = node.aliases;
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

/**
 * Copy a projection node and adapt it to keep all required columns.
 */
std::shared_ptr<ProjectionNode> adapt_projection_node(const ProjectionNode& node,
                                                      const ExpressionUnorderedSet& required_column_expressions) {
  // We don't want to add columns that are already in the projection node. We also don't want to remove duplicates in
  // the expressions of the projection node, so we can't simply build one set containing all expressions
  auto expressions = node.node_expressions;
  ExpressionUnorderedSet original_expressions(expressions.cbegin(), expressions.cend());

  const auto not_found_it = original_expressions.cend();
  for (const auto& expression : required_column_expressions) {
    if (original_expressions.find(expression) == not_found_it) {
      expressions.emplace_back(expression);
    }
  }

  return ProjectionNode::make(expressions);
}

/**
 * Scan the LQP and pull up correlated predicate nodes that we can turn into join predicates.
 */
std::optional<PredicatePullUpInfo> attempt_predicate_pull_up(
    const std::shared_ptr<AbstractLQPNode>& node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping,
    size_t correlated_predicate_node_count, bool is_below_aggregate) {
  // Since we only pull predicates past nodes with one input this is implemented as a linear recursive scan. Once we
  // reach an LQP node that we cannot pull predicates past, we check whether we found all correlated predicate nodes
  // (counted in an earlier step). If not, we return nullopt and unwind. If we found all, we unwind and remove
  // correlated predicate nodes while tracking the columns required by these nodes. We also patch certain nodes (alias,
  // projection, aggregate) to have these columns available at the top of the subquery. Starting from the lowest
  // removed predicate (where we begin changing the LQP) we also copy each node to handle cases where an LQP node is
  // input to multiple other nodes.
  std::shared_ptr<BinaryPredicateExpression> join_predicate;
  switch (node->type) {
    case LQPNodeType::Predicate:
      join_predicate = try_to_extract_join_predicate(std::static_pointer_cast<PredicateNode>(node), parameter_mapping,
                                                     is_below_aggregate);
      if (join_predicate) {
        correlated_predicate_node_count--;
      }
      break;
    case LQPNodeType::Aggregate:
      is_below_aggregate = true;
      break;
    case LQPNodeType::Alias:
    case LQPNodeType::Projection:
    case LQPNodeType::Sort:
    case LQPNodeType::Validate:
      break;
    default:
      // Not safe to pull predicates past this point
      if (correlated_predicate_node_count > 0) {
        // We haven't found all correlated predicate nodes, so there must be some that we cannot safely pull up.
        return std::nullopt;
      }

      // Found all correlated predicate nodes, start copying and adapting the LQP bottom up
      return PredicatePullUpInfo{node, {}, {}};
  }

  DebugAssert(!node->right_input(), "Nodes of these types should only have one input");
  auto maybe_info = attempt_predicate_pull_up(node->left_input(), parameter_mapping, correlated_predicate_node_count,
                                              is_below_aggregate);
  if (!maybe_info) {
    return std::nullopt;
  }

  auto info = *maybe_info;
  if (!join_predicate && info.extracted_join_predicates.empty()) {
    // We are still below the lowest correlated predicate node, no need to change the LQP
    info.adapted_lqp = node;
    return info;
  }

  // Copy and adapt current node
  std::shared_ptr<AbstractLQPNode> copied_node;
  switch (node->type) {
    case LQPNodeType::Predicate: {
      if (join_predicate) {
        // Track new join predicate, remove node from tree (by not updating info.adapted_lqp)
        info.required_column_expressions.emplace(join_predicate->right_operand());
        info.extracted_join_predicates.emplace_back(std::move(join_predicate));
      } else {
        // Uncorrelated predicate node, needs to be copied
        const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
        copied_node = PredicateNode::make(predicate_node->predicate());
      }
      break;
    }
    case LQPNodeType::Aggregate: {
      const auto& aggregate_node = std::static_pointer_cast<AggregateNode>(node);
      copied_node = adapt_aggregate_node(*aggregate_node, info.required_column_expressions);
      break;
    }
    case LQPNodeType::Alias: {
      const auto& alias_node = std::static_pointer_cast<AliasNode>(node);
      copied_node = adapt_alias_node(*alias_node, info.required_column_expressions);
      break;
    }
    case LQPNodeType::Projection: {
      const auto& projection_node = std::static_pointer_cast<ProjectionNode>(node);
      copied_node = adapt_projection_node(*projection_node, info.required_column_expressions);
      break;
    }
    case LQPNodeType::Sort: {
      const auto& sort_node = std::static_pointer_cast<SortNode>(node);
      copied_node = SortNode::make(sort_node->node_expressions, sort_node->order_by_modes);
      break;
    }
    case LQPNodeType::Validate:
      copied_node = ValidateNode::make();
      break;
    default:
      Fail("Unreachable code reached: This node type cannot be adapted and should have been handled earlier");
  }

  if (copied_node) {
    copied_node->set_left_input(info.adapted_lqp);
    info.adapted_lqp = copied_node;
  }

  return info;
}

}  // namespace

std::string SubqueryToJoinRule::name() const { return "Subquery to Join Rule"; }

void SubqueryToJoinRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Check if node contains a subquery and turn it into an anti- or semi-join if possible.
  // To do this, we
  //   - Check whether node is of a supported type:
  //       - (NOT) IN predicate with a subquery as the right operand
  //       - (NOT) EXISTS predicate
  //       - comparison (<,>,<=,>=,=,<>) predicate with subquery as the right operand
  //   - If node is a (NOT) IN or a comparison, extract a first join predicate
  //   - Scan the LQP for all usages of correlated parameters, counting the number of predicate nodes using them (if
  //     one is used outside of predicate nodes, we never optimize the LQP).
  //   - Recursively scan for correlated predicate nodes that we can safely remove (pull up and out of the subquery).
  //     If we find all correlated predicate nodes, we copy and adapt the LQP while unwinding from the recursion.
  //     Changes to non-predicate nodes are necessary to have all columns required by the new join predicates available
  //     at the top of the subqueries LQP.
  //   - Remove found predicates and adjust the subqueries LQP to allow them to be re-inserted as join predicates
  //     (remove projections pruning necessary columns, etc.)
  //   - Build a join with the collected predicates
  //
  // We always reformulate when possible, since benchmarks have shown that this reformulation makes the execution
  // faster regardless of the expected table sizes, etc.

  // Check whether the input LQP is supported, and extract some unified information
  auto maybe_input_info = extract_input_lqp_info(node);
  if (!maybe_input_info) {
    _apply_to_inputs(node);
    return;
  }

  auto input_info = *maybe_input_info;

  // Build a map from parameter ids to their respective expressions.
  std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_mapping;
  for (size_t parameter_idx = 0; parameter_idx < input_info.subquery_expression.parameter_count(); ++parameter_idx) {
    const auto& parameter_expression = input_info.subquery_expression.parameter_expression(parameter_idx);
    parameter_mapping.emplace(input_info.subquery_expression.parameter_ids[parameter_idx], parameter_expression);
  }

  // Scan for unoptimizable correlated parameter usages, and count correlated predicate nodes.
  auto& [not_optimizable, correlated_predicate_node_count] =
      assess_correlated_parameter_usage(input_info.subquery_expression.lqp, parameter_mapping);
  if (not_optimizable) {
    _apply_to_inputs(node);
    return;
  }

  // Attempt to pull up all correlated predicate nodes.
  auto maybe_pull_up_info = attempt_predicate_pull_up(input_info.subquery_expression.lqp, parameter_mapping,
                                                      correlated_predicate_node_count, false);
  if (!maybe_pull_up_info) {
    _apply_to_inputs(node);
    return;
  }

  auto pull_up_info = *maybe_pull_up_info;
  if (input_info.additional_join_predicate) {
    pull_up_info.extracted_join_predicates.emplace_back(std::move(input_info.additional_join_predicate));
  }

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. We check that one exists and move it to the front.
  const auto join_predicate_count = pull_up_info.extracted_join_predicates.size();
  for (size_t predicate_idx = 0; predicate_idx < join_predicate_count; ++predicate_idx) {
    const auto& predicate = pull_up_info.extracted_join_predicates[predicate_idx];
    if (predicate->predicate_condition == PredicateCondition::Equals) {
      std::swap(pull_up_info.extracted_join_predicates.front(), pull_up_info.extracted_join_predicates[predicate_idx]);
      break;
    }
  }

  if (pull_up_info.extracted_join_predicates.empty() ||
      pull_up_info.extracted_join_predicates.front()->predicate_condition != PredicateCondition::Equals) {
    _apply_to_inputs(node);
    return;
  }

  // Need to cast from vector of BinaryPredicateExpressions to vector of AbstractExpressions
  std::vector<std::shared_ptr<AbstractExpression>> join_predicates(pull_up_info.extracted_join_predicates.begin(),
                                                                   pull_up_info.extracted_join_predicates.end());

  // Build final join node
  const auto join_node = JoinNode::make(input_info.join_mode, join_predicates);
  lqp_replace_node(node, join_node);
  join_node->set_right_input(pull_up_info.adapted_lqp);

  _apply_to_inputs(join_node);
}

}  // namespace opossum
