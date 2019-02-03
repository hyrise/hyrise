#include "subselect_to_join_reformulation_rule.hpp"

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
#include "expression/lqp_select_expression.hpp"
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
   * Comparison operand from the right sub-tree (the previous sub-select).
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
   * Projections in the sub-tree which need to be removed to make the predicate pull-up safe.
   */
  std::vector<std::shared_ptr<ProjectionNode>> projections;
};

/**
 * Remove a node from an LQP tree while keeping track of its root.
 *
 * @return The new root
 */
std::shared_ptr<AbstractLQPNode> remove_from_tree(const std::shared_ptr<AbstractLQPNode>& root,
                                                  const std::shared_ptr<AbstractLQPNode>& to_remove) {
  if (root == to_remove) {
    // We only remove nodes without a right input (which is asserted by lqp_remove_node)
    auto new_root = root->left_input();
    lqp_remove_node(to_remove);
    return new_root;
  }

  lqp_remove_node(to_remove);
  return root;
}

/**
 * Checks whether a predicate node should be turned into a join predicate.
 */
std::optional<PredicateInfo> should_become_join_predicate(
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
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

  // Check that one side of the expression is a correlated parameter and the other a column expression of the LQP below
  // the predicate node (required for turning it into a join predicate).
  const auto& binary_predicate_expression = std::static_pointer_cast<BinaryPredicateExpression>(predicate_expression);
  const auto& left_side = binary_predicate_expression->left_operand();
  const auto& right_side = binary_predicate_expression->right_operand();
  PredicateInfo info;
  info.condition = cond_type;
  ParameterID parameter_id;
  if (left_side->type == ExpressionType::Parameter) {
    parameter_id = std::static_pointer_cast<ParameterExpression>(left_side)->parameter_id;
    info.right_operand = right_side;
  } else if (right_side->type == ExpressionType::Parameter) {
    info.condition = flip_predicate_condition(info.condition);
    parameter_id = std::static_pointer_cast<ParameterExpression>(right_side)->parameter_id;
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
 * available for the pulled up predicates.
 */
PredicatePullUpInfo prepare_predicate_pull_up(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& parameter_mapping) {
  // We are only interested in predicate, projection, validate and sort nodes. These only ever have one
  // input, thus we can scan the path of nodes linearly. This makes it easy to track which projection nodes actually
  // need to be removed.
  PredicatePullUpInfo info;
  size_t num_projections_to_remove = 0;

  auto node = lqp;
  while (node != nullptr) {
    if (node->type == LQPNodeType::Projection) {
      info.projections.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    } else if (node->type == LQPNodeType::Predicate) {
      const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
      auto maybe_predicate_info = should_become_join_predicate(predicate_node, parameter_mapping);
      if (maybe_predicate_info) {
        info.predicates.emplace_back(*maybe_predicate_info);
        info.predicate_nodes.emplace(predicate_node);
        // All projections found so far need to be removed
        num_projections_to_remove = info.projections.size();
      }
    } else if (node->type != LQPNodeType::Validate && node->type != LQPNodeType::Sort) {
      // It is not safe to pull up predicates passed this node, stop scanning
      break;
    }

    DebugAssert(!node->right_input(), "Scan only implemented for nodes with one input");
    node = node->left_input();
  }

  // Remove projections found below the last predicate which we don't need to remove.
  info.projections.resize(num_projections_to_remove);
  return info;
}

/**
 * Check whether an LQP node uses a correlated parameter.
 *
 * If the node uses a sub-select, its nodes are also all checked.
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

      if (sub_expression->type == ExpressionType::LQPSelect) {
        // Need to check whether the sub-select uses correlated parameters
        const auto& lqp_select_expression = std::static_pointer_cast<LQPSelectExpression>(sub_expression);
        visit_lqp(lqp_select_expression->lqp, [&](const auto& sub_node) {
          if (is_correlated) {
            return LQPVisitation::DoNotVisitInputs;
          }

          is_correlated |= uses_correlated_parameters(sub_node, is_correlated_parameter);
          return is_correlated ? LQPVisitation::DoNotVisitInputs : LQPVisitation::VisitInputs;
        });
      } else if (sub_expression->type == ExpressionType::Parameter) {
        const auto& parameter_expression = std::static_pointer_cast<ParameterExpression>(sub_expression);
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

std::string SubselectToJoinReformulationRule::name() const { return "Sub-select to Join Reformulation Rule"; }

bool SubselectToJoinReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (NOT)-IN or (NOT)-EXISTS predicates
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();

  const auto& left_tree_root = node->left_input();
  std::vector<std::shared_ptr<BinaryPredicateExpression>> join_predicates;
  std::shared_ptr<LQPSelectExpression> subselect_expression;
  JoinMode join_mode;
  if (predicate_node_predicate->type == ExpressionType::Predicate) {
    // Check that the predicate is a (NOT) IN
    const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node_predicate);
    if (predicate_expression->predicate_condition != PredicateCondition::In &&
        predicate_expression->predicate_condition != PredicateCondition::NotIn) {
      return _apply_to_inputs(node);
    }

    const auto in_expression = std::static_pointer_cast<InExpression>(predicate_expression);
    // Only optimize if the set is a sub-select, and not a static list
    if (in_expression->set()->type != ExpressionType::LQPSelect) {
      return _apply_to_inputs(node);
    }

    subselect_expression = std::static_pointer_cast<LQPSelectExpression>(in_expression->set());
    // Find the single column that the sub-query should produce and turn it into one of our join predicates.
    const auto right_column_expressions = subselect_expression->lqp->column_expressions();
    if (right_column_expressions.size() != 1) {
      return _apply_to_inputs(node);
    }

    auto right_join_expression = right_column_expressions[0];
    // Check that the in value is a column expression of the left sub-tree. This is required it to be used as a side in
    // a join predicate. The right join expression doesn't need to be checked because it obviously is a column
    // expression.
    if (!left_tree_root->find_column_id(*in_expression->value())) {
      return _apply_to_inputs(node);
    }

    join_predicates.emplace_back(std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::Equals, in_expression->value(), right_join_expression));
    join_mode = in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
  } else if (predicate_node_predicate->type == ExpressionType::Exists) {
    const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node_predicate);
    auto exists_sub_select = exists_expression->select();

    // TODO(anybody): According to the source for ExistsExpression this could also be a PQPSelectExpression. Could this
    // be actually be the case here, or should the check be removed?
    if (exists_sub_select->type != ExpressionType::LQPSelect) {
      return _apply_to_inputs(node);
    }

    subselect_expression = std::static_pointer_cast<LQPSelectExpression>(exists_sub_select);

    // We cannot optimize uncorrelated exists into a join
    if (!subselect_expression->is_correlated()) {
      return _apply_to_inputs(node);
    }

    // TODO(anybody): Could ExistsExpressionType ever have more than two possible values?
    join_mode =
        exists_expression->exists_expression_type == ExistsExpressionType::Exists ? JoinMode::Semi : JoinMode::Anti;
  } else {
    return _apply_to_inputs(node);
  }

  // Keep track of the root of right tree when removing projections and predicates
  auto right_tree_root = subselect_expression->lqp;

  // We cannot support anti joins right now (see large comment below on multi-predicate joins for the reasons).
  if (join_mode != JoinMode::Semi) {
    return _apply_to_inputs(node);
  }

  // Do not reformulate if expected output is small.
  if (node->get_statistics()->row_count() < 150.0f) {
    return _apply_to_inputs(node);
  }
  std::cout << "node cost before in reformulation: " << CostModelLogical().estimate_plan_cost(node) << '\n';

  // TODO(anybody): Is this check actually necessary, or is this always true for correlated parameters?
  // Check that all correlated parameters expressions are column expressions of the left sub-tree. Otherwise, we won't
  // be able to use them in join predicates. Also build up a map from parameter ids to their respective expressions.
  std::map<ParameterID, std::shared_ptr<AbstractExpression>> parameter_mapping;
  for (size_t parameter_idx = 0; parameter_idx < subselect_expression->parameter_count(); ++parameter_idx) {
    const auto& parameter_expression = subselect_expression->parameter_expression(parameter_idx);
    if (!left_tree_root->find_column_id(*parameter_expression)) {
      return _apply_to_inputs(node);
    }

    parameter_mapping.emplace(subselect_expression->parameter_ids[parameter_idx], parameter_expression);
  }

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
  for (const auto& projection_node : pull_up_info.projections) {
    right_tree_root = remove_from_tree(right_tree_root, projection_node);
  }

  for (const auto& correlated_predicate_node : pull_up_info.predicate_nodes) {
    right_tree_root = remove_from_tree(right_tree_root, correlated_predicate_node);
  }

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

  std::cout << "node cost after in reformulation: " << CostModelLogical().estimate_plan_cost(distinct_node) << '\n';

  return _apply_to_inputs(distinct_node);
}

}  // namespace opossum
