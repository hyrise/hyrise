#include "subquery_to_join_reformulation_rule.hpp"

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

/**
 * Info about a predicate which needs to be pulled up into a join predicate.
 */
struct PredicateInfo {
  /**
   * Comparison operand from the left sub-tree.
   */
  std::shared_ptr<AbstractExpression> left_operand;

  /**
   * Comparison operand from the right sub-tree (the previous subquery).
   */
  std::shared_ptr<AbstractExpression> right_operand;

  PredicateCondition condition;
};

/**
 * Collected information about the predicates which need to be pulled up and other nodes which need to be adjusted.
 */
struct PredicatePullUpInfo {
  /**
   * Predicates to pull up.
   *
   * Ordered by depth in the sub-tree, from top to bottom.
   */
  std::vector<PredicateInfo> predicates;

  /**
   * Nodes from which the pull up predicates originate.
   */
  std::set<std::shared_ptr<PredicateNode>> predicate_nodes;

  /**
   * Projection nodes in the sub-tree which need to be removed to make the predicate pull-up safe.
   */
  std::vector<std::shared_ptr<ProjectionNode>> projection_nodes;

  /**
   * Alias nodes in the sub-tree which need to be extended to make the predicate pull-up safe.
   *
   * The nodes are ordered top to bottom.
   */
  std::vector<std::shared_ptr<AliasNode>> alias_nodes;

  /**
   * Aggregates which need to be patched allow the predicates to be pulled up.
   *
   * Ordered by depth in the sub-tree, from top to bottom. The number notes the number of predicates above this
   * aggregate in the tree, which won't need be considered when patching the aggregate.
   */
  std::vector<std::pair<std::shared_ptr<AggregateNode>, size_t>> aggregate_nodes;
};

/**
 * Checks whether a predicate node should be turned into a join predicate.
 */
std::optional<PredicateInfo> should_become_join_predicate(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping, bool is_below_aggregate) {
  // Check for the type of expression first. Note that we are not concerned with predicates of other forms using
  // correlated parameters here. We check for parameter usages that prevent optimization later in
  // contains_unoptimizable_correlated_parameter_usages.
  if (predicate_node->predicate()->type != ExpressionType::Predicate) {
    return std::nullopt;
  }

  const auto& predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());

  // Joins only support these six binary predicates. We rely on LogicalReductionRule having split up ANDed chains of
  // such predicates previously, so that we can process them separately.
  auto cond_type = predicate_expression->predicate_condition;
  if (cond_type != PredicateCondition::Equals && cond_type != PredicateCondition::NotEquals &&
      cond_type != PredicateCondition::LessThan && cond_type != PredicateCondition::LessThanEquals &&
      cond_type != PredicateCondition::GreaterThan && cond_type != PredicateCondition::GreaterThanEquals) {
    return std::nullopt;
  }

  // We can currently only pull equals predicates above aggregate nodes. The other predicate types could be supported
  // but require more sophisticated reformulations.
  if (is_below_aggregate && cond_type != PredicateCondition::Equals) {
    return std::nullopt;
  }

  // Check that one side of the expression is a correlated parameter and the other a column expression of the LQP below
  // the predicate node (required for turning it into a join predicate).
  const auto& binary_predicate_expression = std::static_pointer_cast<BinaryPredicateExpression>(predicate_expression);
  const auto& left_side = binary_predicate_expression->left_operand();
  const auto& right_side = binary_predicate_expression->right_operand();
  PredicateInfo info;
  info.condition = cond_type;
  ParameterID parameter_id;
  if (left_side->type == ExpressionType::CorrelatedParameter) {
    parameter_id = std::static_pointer_cast<CorrelatedParameterExpression>(left_side)->parameter_id;
    info.right_operand = right_side;
  } else if (right_side->type == ExpressionType::CorrelatedParameter) {
    info.condition = flip_predicate_condition(info.condition);
    parameter_id = std::static_pointer_cast<CorrelatedParameterExpression>(right_side)->parameter_id;
    info.right_operand = left_side;
  } else {
    return std::nullopt;
  }

  if (!predicate_node->find_column_id(*info.right_operand)) {
    return std::nullopt;
  }

  auto expression_it = parameter_mapping.find(parameter_id);
  if (expression_it == parameter_mapping.end()) {
    return std::nullopt;
  }

  info.left_operand = expression_it->second;
  return info;
}

/**
 * Finds predicate nodes to pull up to become join predicates and other nodes which need to be removed or patched.
 *
 * This selects all predicates that can be safely pulled up and turned into join predicates. It also collects all
 * projection nodes above the selected predicates. These need to be removed to make sure that the required columns are
 * available for the pulled up predicates. Finally, it collects aggregate nodes which need to be patched with
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
      auto maybe_predicate_info =
          should_become_join_predicate(predicate_node, parameter_mapping, !info.aggregate_nodes.empty());
      if (maybe_predicate_info) {
        info.predicates.emplace_back(*maybe_predicate_info);
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

  // Remove projections/aggregates found below the last predicate which we don't need to remove/patch.
  info.projection_nodes.resize(num_projections_to_remove);
  info.aggregate_nodes.resize(num_aggregates_to_patch);

  return info;
}

/**
 * Check whether an LQP node uses a correlated parameter.
 *
 * If the node uses a subquery, its nodes are also all checked.
 */
template <class ParameterPredicate>
bool uses_correlated_parameters(const std::shared_ptr<AbstractLQPNode>& node,
                                ParameterPredicate&& is_correlated_parameter) {
  bool is_correlated = false;
  for (const auto& expression : node->node_expressions) {
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

          is_correlated |= uses_correlated_parameters(sub_node, is_correlated_parameter);
          return is_correlated ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
        });
      } else if (sub_expression->type == ExpressionType::CorrelatedParameter) {
        const auto& parameter_expression = std::static_pointer_cast<CorrelatedParameterExpression>(sub_expression);
        is_correlated |= is_correlated_parameter(parameter_expression->parameter_id);
      }

      return is_correlated ? ExpressionVisitation::DoNotVisitArguments : ExpressionVisitation::VisitArguments;
    });

    if (is_correlated) {
      break;
    }
  }

  return is_correlated;
}

/**
 * Searches for usages of correlated parameters that we cannot optimize.
 *
 * This includes two things:
 *   - Usages of correlated parameters outside of predicate nodes (for example joins)
 *   - Usages of correlated parameters in predicate nodes nested below nodes they cannot be pulled up past.
 */
template <class ParameterPredicate>
bool contains_unoptimizable_correlated_parameter_usages(
    const std::shared_ptr<AbstractLQPNode>& lqp, ParameterPredicate&& is_correlated_parameter,
    const std::set<std::shared_ptr<PredicateNode>> optimizable_predicate_nodes) {
  bool optimizable = true;
  visit_lqp(lqp, [&](const auto& node) {
    if (!optimizable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    auto correlated = uses_correlated_parameters(node, is_correlated_parameter);
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
    group_by_expressions.emplace_back(pull_up_info.predicates[predicate_idx].right_operand);

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
 * Replace alias nodes by ones which preserve all columns from below.
 */
void replace_alias_nodes(PredicatePullUpInfo& pull_up_info) {
  // Replace the nodes bottom to top, in case of two alias nodes being adjacent.
  auto& alias_nodes = pull_up_info.alias_nodes;
  for (auto alias_it = alias_nodes.rbegin(), alias_it_end = alias_nodes.rend(); alias_it != alias_it_end; ++alias_it) {
    auto& alias_node = *alias_it;
    // Copy all the expressions with their aliases from the alias node. Then add all missing expressions from the alias
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

std::string SubqueryToJoinReformulationRule::name() const { return "Subquery to Join Reformulation Rule"; }

void SubqueryToJoinReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (NOT)-IN or (NOT)-EXISTS predicates
  if (node->type != LQPNodeType::Predicate) {
    _apply_to_inputs(node);
    return;
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();

  const auto& left_tree_root = node->left_input();
  std::vector<std::shared_ptr<BinaryPredicateExpression>> join_predicates;
  std::shared_ptr<LQPSubqueryExpression> subquery_expression;
  JoinMode join_mode;

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
      join_mode = in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
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
    if (right_column_expressions.size() != 1) {
      _apply_to_inputs(node);
      return;
    }

    join_predicates.emplace_back(std::make_shared<BinaryPredicateExpression>(
        comparison_condition, comparison_expression, right_column_expressions.front()));
  } else if (predicate_node_predicate->type == ExpressionType::Exists) {
    const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node_predicate);
    auto exists_sub_select = exists_expression->subquery();

    // TODO(anybody): According to the source for ExistsExpression this could also be a PQPSubqueryExpression. Could
    //  this be actually be the case here, or should the check be removed?
    if (exists_sub_select->type != ExpressionType::LQPSubquery) {
      _apply_to_inputs(node);
      return;
    }

    subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(exists_sub_select);

    // We cannot optimize uncorrelated exists into a join
    if (!subquery_expression->is_correlated()) {
      _apply_to_inputs(node);
      return;
    }

    // TODO(anybody): Could ExistsExpressionType ever have more than two possible values?
    join_mode =
        exists_expression->exists_expression_type == ExistsExpressionType::Exists ? JoinMode::Semi : JoinMode::Anti;
  } else {
    _apply_to_inputs(node);
    return;
  }

  auto right_tree_root = subquery_expression->lqp;

  // We cannot support anti joins right now (see large comment below on multi-predicate joins for the reasons).
  if (join_mode != JoinMode::Semi) {
    _apply_to_inputs(node);
    return;
  }

  // Do not reformulate if expected output is small.
  if (node->get_statistics()->row_count() < 100.0f) {
    _apply_to_inputs(node);
    return;
  }
  std::cout << "node cost before in reformulation: " << CostModelLogical().estimate_plan_cost(node) << '\n';

  // TODO(anybody): Is this check actually necessary, or is this always true for correlated parameters?
  // Check that all correlated parameters expressions are column expressions of the left sub-tree. Otherwise, we won't
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
  //    - Alias nodes are due to the same reasons as projection nodes. Since we can't remove them (that would remove
  //      the aliased columns) we simply extend them to only add and not remove any columns.
  //    - Aggregate nodes also remove columns. For now, we only support pulling equals predicates above them. For that
  //      to work, we extend the aggregate nodes to also group by the columns referenced by all predicates pulled from
  //      below to above them.
  //
  // The only other nodes we pull predicates past are sort and validate nodes, which don't need to be adjusted.
  auto pull_up_info = prepare_predicate_pull_up(right_tree_root, parameter_mapping);
  auto is_correlated_parameter = [&](ParameterID id) { return parameter_mapping.find(id) != parameter_mapping.end(); };
  if (contains_unoptimizable_correlated_parameter_usages(right_tree_root, is_correlated_parameter,
                                                         pull_up_info.predicate_nodes)) {
    return _apply_to_inputs(node);
  }

  // Collect all join predicates into one place
  for (auto& predicate_info : pull_up_info.predicates) {
    // Note that we cannot remove the predicate nodes here just yet, because the reformulation might still fail when no
    // valid primary join predicate is found.
    join_predicates.emplace_back(std::make_shared<BinaryPredicateExpression>(
        predicate_info.condition, predicate_info.left_operand, predicate_info.right_operand));
  }

  // Semi and anti joins are currently only implemented by hash joins. These need an equals comparison as the primary
  // join predicate.
  for (auto it = join_predicates.begin(), end = join_predicates.end(); it != end; ++it) {
    const auto& predicate = **it;
    if (predicate.predicate_condition == PredicateCondition::Equals) {
      std::iter_swap(join_predicates.begin(), it);
      break;
    }
  }

  if (join_predicates.empty() || join_predicates.front()->predicate_condition != PredicateCondition::Equals) {
    return _apply_to_inputs(node);
  }

  // Begin altering the LQP. All failure checks need to be finished at this point.
  //
  // Since multi-predicate joins are currently still work-in-progress, we emulate them by:
  //   - Performing an inner join on the two trees using an equals predicate
  //   - Putting the remaining predicates in predicate nodes above the join
  //   - Inserting a projection above the predicates to filter out any columns from the right sub-tree
  //   - Inserting a group by over all columns from the left subtree, to filter out duplicates introduced by the join
  //
  // NOTE: This only works correctly if the left sub-tree does not contain any duplicates. It also only works for
  // emulating semi joins, not for anti joins. It is only meant to get the implementation started and to collect some
  // preliminary benchmark results.
  const auto left_columns = predicate_node->left_input()->column_expressions();
  auto distinct_node = AggregateNode::make(left_columns, std::vector<std::shared_ptr<AbstractExpression>>{});
  auto left_only_projection_node = ProjectionNode::make(left_columns);
  const auto join_node = JoinNode::make(JoinMode::Inner, std::move(join_predicates.front()));
  join_predicates.erase(join_predicates.begin());

  lqp_replace_node(node, distinct_node);
  lqp_insert_node(distinct_node, LQPInputSide::Left, left_only_projection_node);

  std::shared_ptr<AbstractLQPNode> parent = left_only_projection_node;
  for (auto& secondary_join_predicate : join_predicates) {
    const auto secondary_predicate_node = PredicateNode::make(std::move(secondary_join_predicate));
    lqp_insert_node(parent, LQPInputSide::Left, secondary_predicate_node);
    parent = secondary_predicate_node;
  }

  lqp_insert_node(parent, LQPInputSide::Left, join_node);
  join_node->set_right_input(right_tree_root);

  // By removing/replacing nodes only now, the right input of the join node will be automatically kept up to date when
  // we remove/replace the root of the previous subquery.
  for (const auto& projection_node : pull_up_info.projection_nodes) {
    lqp_remove_node(projection_node);
  }

  for (const auto& correlated_predicate_node : pull_up_info.predicate_nodes) {
    lqp_remove_node(correlated_predicate_node);
  }

  replace_aggregate_nodes(pull_up_info);
  replace_alias_nodes(pull_up_info);

  std::cout << "node cost after in reformulation: " << CostModelLogical().estimate_plan_cost(distinct_node) << '\n';

  _apply_to_inputs(distinct_node);
}

}  // namespace opossum
