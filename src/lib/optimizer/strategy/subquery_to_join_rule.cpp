#include "subquery_to_join_rule.hpp"

#include <algorithm>
#include <map>
#include <memory>
#include <set>

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
#include "statistics/table_statistics.hpp"
#include "utils/assert.hpp"

namespace opossum {

namespace {

/**
 * Collected information about the predicates that need to be pulled up and other nodes that need to be adjusted.
 */
struct PredicatePullUpInfo {
  /**
   * Predicates to pull up.
   *
   * Ordered by depth in the subtree, from top to bottom. Each predicate has the column reference from the left subtree
   * as its `left_operand()` and respectively for `right_operand()`.
   */
  std::vector<std::shared_ptr<BinaryPredicateExpression>> predicates;

  /**
   * Nodes that contain the predicates in the subqueries LQP.
   */
  std::set<std::shared_ptr<PredicateNode>> predicate_nodes;

  /**
   * Projection nodes in the subtree that need to be removed to make the predicate pull-up safe.
   */
  std::vector<std::shared_ptr<ProjectionNode>> projection_nodes;

  /**
   * Alias nodes in the subtree that need to be extended to make the predicate pull-up safe.
   *
   * The nodes are ordered top to bottom.
   */
  std::vector<std::shared_ptr<AliasNode>> alias_nodes;

  /**
   * Aggregates that need to be patched allow the predicates to be pulled up.
   *
   * Ordered by depth in the subtree, from top to bottom. The number notes the number of predicates above this
   * aggregate in the LQP.
   */
  std::vector<std::pair<std::shared_ptr<AggregateNode>, size_t>> aggregate_nodes;
};

/**
 * Checks whether a predicate node should be turned into a join predicate.
 */
std::shared_ptr<BinaryPredicateExpression> should_become_join_predicate(
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
  if (predicate_condition != PredicateCondition::Equals && predicate_condition != PredicateCondition::NotEquals &&
      predicate_condition != PredicateCondition::LessThan &&
      predicate_condition != PredicateCondition::LessThanEquals &&
      predicate_condition != PredicateCondition::GreaterThan &&
      predicate_condition != PredicateCondition::GreaterThanEquals) {
    return nullptr;
  }

  // We can currently only pull equals predicates above aggregate nodes. The other predicate types could be supported
  // but require more sophisticated reformulations.
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
 * Finds predicate nodes to pull up to become join predicates and other nodes that need to be removed or patched.
 *
 * This selects all predicates that can be safely pulled up and turned into join predicates. It also collects all
 * projection nodes above the selected predicates. These need to be removed to make sure that the required columns are
 * available for the pulled up predicates. Finally, it collects aggregate nodes that need to be patched with
 * additional group by columns to allow the predicates to be pulled up.
 */
PredicatePullUpInfo prepare_predicate_pull_up(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  // We are only interested in predicate, projection, validate, aggregate and sort nodes. These only ever have one
  // input, thus we can scan the path of nodes linearly. This makes it easy to track which projection nodes actually
  // need to be removed, and which predicates need to be considered when patching aggregates.
  PredicatePullUpInfo info{};
  size_t num_projections_to_remove = 0;
  size_t num_aggregates_to_patch = 0;

  auto node = lqp;
  while (node != nullptr) {
    if (node->type == LQPNodeType::Projection) {
      info.projection_nodes.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    } else if (node->type == LQPNodeType::Predicate) {
      const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
      auto maybe_predicate =
          should_become_join_predicate(predicate_node, parameter_mapping, !info.aggregate_nodes.empty());
      if (maybe_predicate) {
        info.predicates.emplace_back(std::move(maybe_predicate));
        info.predicate_nodes.emplace(predicate_node);
        // All projections/aggregates found so far need to be removed/patched
        num_projections_to_remove = info.projection_nodes.size();
        num_aggregates_to_patch = info.aggregate_nodes.size();
      }
    } else if (node->type == LQPNodeType::Aggregate) {
      info.aggregate_nodes.emplace_back(std::static_pointer_cast<AggregateNode>(node), info.predicates.size());
    } else if (node->type == LQPNodeType::Alias) {
      info.alias_nodes.emplace_back(std::static_pointer_cast<AliasNode>(node));
    } else if (node->type != LQPNodeType::Validate && node->type != LQPNodeType::Sort) {
      // It is not safe to pull up predicates past this node, stop scanning
      break;
    }

    DebugAssert(!node->right_input(), "Scan only implemented for nodes with one input");
    node = node->left_input();
  }

  // Remove projections/aggregates found below the last predicate that we don't need to remove/patch.
  info.projection_nodes.resize(num_projections_to_remove);
  info.aggregate_nodes.resize(num_aggregates_to_patch);

  return info;
}

/**
 * Check whether an LQP node uses a correlated parameter.
 *
 * If the node uses a subquery, its nodes are also all checked.
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

      if (sub_expression->type == ExpressionType::LQPSubquery) {
        // Need to check whether the subquery uses correlated parameters
        const auto& lqp_select_expression = std::static_pointer_cast<LQPSubqueryExpression>(sub_expression);
        visit_lqp(lqp_select_expression->lqp, [&](const auto& sub_node) {
          if (is_correlated) {
            return LQPVisitation::DoNotVisitInputs;
          }

          is_correlated |= uses_correlated_parameters(sub_node, parameter_mapping);
          return is_correlated ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
        });
      } else if (sub_expression->type == ExpressionType::CorrelatedParameter) {
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
 * Searches for usages of correlated parameters that we cannot optimize.
 *
 * This includes two things:
 *   - Usages of correlated parameters outside of predicate nodes (for example joins)
 *   - Usages of correlated parameters in predicate nodes nested below nodes they cannot be pulled up past.
 */
bool contains_unoptimizable_correlated_parameter_usages(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping,
    const std::set<std::shared_ptr<PredicateNode>> optimizable_predicate_nodes) {
  bool optimizable = true;
  visit_lqp(lqp, [&](const auto& node) {
    if (!optimizable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    auto correlated = uses_correlated_parameters(node, parameter_mapping);
    if (correlated) {
      if (node->type != LQPNodeType::Predicate ||
          optimizable_predicate_nodes.find(std::static_pointer_cast<PredicateNode>(node)) ==
              optimizable_predicate_nodes.end()) {
        optimizable = false;
        return LQPVisitation::DoNotVisitInputs;
      }
    }

    return LQPVisitation::VisitInputs;
  });

  return !optimizable;
}

/**
 * Replace aggregate nodes by ones with additional group by expressions added to allow predicates to be pulled up.
 */
void replace_aggregate_nodes(PredicatePullUpInfo& pull_up_info) {
  // Walk the predicates from bottom to top while keeping track of the column expressions they need to be available.
  // Whenever an aggregate node is encountered, replace it by a new one with the column expressions of all the
  // predicates below it added as group by expressions.

  // TODO(anybody): This code does quite a bit of unnecessary copying around because the AggregateNode constructor
  // expects two expression vectors (which it then copies again).
  std::vector<std::shared_ptr<AbstractExpression>> group_by_expressions;
  auto aggregate_it = pull_up_info.aggregate_nodes.rbegin();
  auto aggregate_it_end = pull_up_info.aggregate_nodes.rend();
  for (auto predicate_idx = pull_up_info.predicates.size(); predicate_idx--;) {
    group_by_expressions.emplace_back(pull_up_info.predicates[predicate_idx]->right_operand());

    while (aggregate_it != aggregate_it_end && aggregate_it->second == predicate_idx) {
      auto& aggregate_node = aggregate_it->first;
      // Add all group by expressions of the original aggregate node unless they are already in the vector. After
      // creating the new aggregate node, the added expressions will be removed again.
      auto original_size = group_by_expressions.size();
      for (size_t group_by_idx = 0; group_by_idx < aggregate_node->aggregate_expressions_begin_idx; ++group_by_idx) {
        auto& group_by_expression = aggregate_node->node_expressions[group_by_idx];
        auto it = std::find(group_by_expressions.begin(), group_by_expressions.end(), group_by_expression);
        if (it == group_by_expressions.end()) {
          group_by_expressions.emplace_back(group_by_expression);
        }
      }

      std::vector<std::shared_ptr<AbstractExpression>> aggregate_expressions(
          aggregate_node->node_expressions.begin() + aggregate_node->aggregate_expressions_begin_idx,
          aggregate_node->node_expressions.end());
      auto patched_aggregate_node = AggregateNode::make(group_by_expressions, aggregate_expressions);
      lqp_replace_node(aggregate_node, patched_aggregate_node);

      // Reset list of group by expressions
      group_by_expressions.resize(original_size);
      aggregate_it++;
    }
  }
}

/**
 * Replace alias nodes by ones that preserve all columns from below.
 */
void replace_alias_nodes(PredicatePullUpInfo& pull_up_info) {
  // Replace the nodes bottom to top, in case of two consecutive alias nodes.
  auto& alias_nodes = pull_up_info.alias_nodes;
  for (auto alias_it = alias_nodes.rbegin(), alias_it_end = alias_nodes.rend(); alias_it != alias_it_end; ++alias_it) {
    auto& alias_node = *alias_it;
    // Copy all expressions with their aliases from the alias node. Then add all missing expressions from the alias
    // nodes input node with as_column_name() as the alias.
    auto expressions = alias_node->node_expressions;
    auto aliases = alias_node->aliases;

    const auto& input_node = alias_node->left_input();
    for (const auto& expression : input_node->column_expressions()) {
      auto it = std::find(alias_node->node_expressions.begin(), alias_node->node_expressions.end(), expression);
      if (it == alias_node->node_expressions.end()) {
        expressions.emplace_back(expression);
        aliases.emplace_back(expression->as_column_name());
      }
    }

    auto new_alias_node = AliasNode::make(std::move(expressions), std::move(aliases));
    lqp_replace_node(alias_node, new_alias_node);
  }
}

}  // namespace

std::string SubqueryToJoinRule::name() const { return "Subquery to Join Rule"; }

void SubqueryToJoinRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Check if node contains a subquery and turn it into an anti- or semi-join if possible.
  // To do this, we
  //   - Check whether node is of a supported type ((NOT) IN, (NOT) EXISTS, comparison with subquery)
  //   - If node is a (NOT) IN or comparison, extract a first join predicate
  //   - Find predicates using correlated parameters (if the subquery has ones) and check whether they can be pulled up
  //     to be turned into additional join predicates
  //   - Check if there are other uses of correlated parameters that we cannot optimize, for example in joins.
  //   - Remove found predicates and adjust the subqueries LQP to allow them to be re-inserted as join predicates
  //     (remove projections pruning necessary columns, etc.)
  //   - Build a join with the collected predicates

  // Filter out all nodes that are not (NOT)-IN or (NOT)-EXISTS predicates
  if (node->type != LQPNodeType::Predicate) {
    _apply_to_inputs(node);
    return;
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();

  const auto& left_tree_root = node->left_input();
  std::shared_ptr<BinaryPredicateExpression> additional_join_predicate;
  std::shared_ptr<LQPSubqueryExpression> subquery_expression;
  JoinMode join_mode;

  if (predicate_node_predicate->type != ExpressionType::Predicate &&
      predicate_node_predicate->type != ExpressionType::Exists) {
    _apply_to_inputs(node);
    return;
  }

  if (predicate_node_predicate->type == ExpressionType::Predicate) {
    const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node_predicate);
    auto predicate_condition = predicate_expression->predicate_condition;

    std::shared_ptr<AbstractExpression> comparison_expression;
    PredicateCondition comparison_condition;
    if (predicate_condition == PredicateCondition::In || predicate_condition == PredicateCondition::NotIn) {
      const auto in_expression = std::static_pointer_cast<InExpression>(predicate_expression);
      // Only optimize if the set is a subquery and not a static list
      if (in_expression->set()->type != ExpressionType::LQPSubquery) {
        _apply_to_inputs(node);
        return;
      }

      subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(in_expression->set());
      comparison_expression = in_expression->value();
      comparison_condition = PredicateCondition::Equals;
      join_mode = in_expression->is_negated() ? JoinMode::AntiDiscardNulls : JoinMode::Semi;
    } else if (predicate_condition == PredicateCondition::Equals ||
               predicate_condition == PredicateCondition::NotEquals ||
               predicate_condition == PredicateCondition::LessThan ||
               predicate_condition == PredicateCondition::LessThanEquals ||
               predicate_condition == PredicateCondition::GreaterThan ||
               predicate_condition == PredicateCondition::GreaterThanEquals) {
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
        _apply_to_inputs(node);
        return;
      }
    } else {
      _apply_to_inputs(node);
      return;
    }

    // Check that the comparison expression is a column expression of the left input tree so that it can be turned into
    // a join predicate.
    if (!left_tree_root->find_column_id(*comparison_expression)) {
      _apply_to_inputs(node);
      return;
    }

    // Check that the subquery returns a single column, and build a join predicate with it.
    const auto& right_column_expressions = subquery_expression->lqp->column_expressions();
    Assert(right_column_expressions.size() == 1, "IN/comparison subquery should only return a single column");
    additional_join_predicate = std::make_shared<BinaryPredicateExpression>(comparison_condition, comparison_expression,
                                                                            right_column_expressions.front());
  } else {
    // Predicate must be a (NOT) EXISTS (checked in guard clause above)
    const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node_predicate);
    auto exists_subquery = exists_expression->subquery();

    Assert(exists_subquery->type == ExpressionType::LQPSubquery,
           "Optimization rule should be run before LQP translation");
    subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(exists_subquery);

    // We cannot optimize uncorrelated exists into a join
    if (!subquery_expression->is_correlated()) {
      _apply_to_inputs(node);
      return;
    }

    join_mode = exists_expression->exists_expression_type == ExistsExpressionType::Exists ? JoinMode::Semi
                                                                                          : JoinMode::AntiRetainNulls;
  }

  auto right_tree_root = subquery_expression->lqp;

  // Do not reformulate if expected output is small.
  //  if (node->get_statistics()->row_count() < 100.0f) {
  //    _apply_to_inputs(node);
  //    return;
  //  }

  // TODO(anybody): Is this check actually necessary, or is this always true for correlated parameters?
  // Check that all correlated parameters expressions are column expressions of the left subtree. Otherwise, we won't
  // be able to use them in join predicates. Also build up a map from parameter ids to their respective expressions.
  std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_mapping;
  for (size_t parameter_idx = 0; parameter_idx < subquery_expression->parameter_count(); ++parameter_idx) {
    const auto& parameter_expression = subquery_expression->parameter_expression(parameter_idx);
    if (!left_tree_root->find_column_id(*parameter_expression)) {
      _apply_to_inputs(node);
      return;
    }

    parameter_mapping.emplace(subquery_expression->parameter_ids[parameter_idx], parameter_expression);
  }

  // Find usages of correlated parameters and try to turn them into join predicates. To pull up the predicates safely,
  // other nodes might need to be adjusted:
  //    - Projection nodes might remove the required columns, so they are simply removed. Since we join with a
  //      semi/anti join, this does not change the semantics. We run before ColumnPruningRule, which then re-adds
  //      appropriate pruning projections.
  //    - Alias nodes are problematic due to the same reasons as projection nodes. Since we can't remove them (that
  //      would remove the aliased columns) we simply extend them to only add and not remove any columns.
  //    - Aggregate nodes also remove columns. For now, we only support pulling equals predicates above them. For that
  //      to work, we extend the aggregate nodes to also group by the columns referenced by all predicates pulled from
  //      below to above them.
  //
  // The only other nodes we pull predicates past are sort and validate nodes, which don't need to be adjusted.
  auto pull_up_info = prepare_predicate_pull_up(right_tree_root, parameter_mapping);
  if (contains_unoptimizable_correlated_parameter_usages(right_tree_root, parameter_mapping,
                                                         pull_up_info.predicate_nodes)) {
    return _apply_to_inputs(node);
  }

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate. We check that one exists and move it to the front.
  for (auto it = pull_up_info.predicates.begin(), end = pull_up_info.predicates.end(); it != end; ++it) {
    const auto& predicate = **it;
    if (predicate.predicate_condition == PredicateCondition::Equals) {
      std::iter_swap(pull_up_info.predicates.begin(), it);
      break;
    }
  }

  if (pull_up_info.predicates.empty() ||
      pull_up_info.predicates.front()->predicate_condition != PredicateCondition::Equals) {
    return _apply_to_inputs(node);
  }

  // Begin altering the LQP. All failure checks need to be finished at this point.
  std::vector<std::shared_ptr<AbstractExpression>> join_predicates(pull_up_info.predicates.cbegin(),
                                                                   pull_up_info.predicates.cend());
  if (additional_join_predicate) {
    join_predicates.emplace_back(std::move(additional_join_predicate));
  }

  const auto join_node = JoinNode::make(join_mode, join_predicates);
  lqp_replace_node(node, join_node);
  join_node->set_right_input(right_tree_root);
  for (const auto& projection_node : pull_up_info.projection_nodes) {
    lqp_remove_node(projection_node);
  }

  for (const auto& correlated_predicate_node : pull_up_info.predicate_nodes) {
    lqp_remove_node(correlated_predicate_node);
  }

  replace_aggregate_nodes(pull_up_info);
  replace_alias_nodes(pull_up_info);

  _apply_to_inputs(join_node);
}

}  // namespace opossum
