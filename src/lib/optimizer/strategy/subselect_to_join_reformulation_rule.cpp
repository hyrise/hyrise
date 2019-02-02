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
 *
 * This checks whether the predicate uses a correlated parameter and whether it uses a supported type of expression.
 */
template <class ParameterPredicate>
bool should_become_join_predicate(const std::shared_ptr<PredicateNode>& predicate_node,
                                  ParameterPredicate&& is_correlated_parameter) {
  // TODO(anybody): Extend this to match whatever is supported by the multi-predicate join implementation.

  // Check for the type of expression first. Note that we are not concerned with predicates of other forms using
  // correlated parameters here. We check for parameter usages that prevent optimization later in
  // contains_unoptimizable_correlated_parameter_usages.
  if (predicate_node->predicate()->type != ExpressionType::Predicate) {
    return false;
  }

  const auto& predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate_node->predicate());

  // We only support binary predicates. We rely on LogicalReductionRule having split up ANDed chains of such
  // expressions previously, so that we can process them separately.
  auto cond_type = predicate_expression->predicate_condition;
  if (cond_type != PredicateCondition::Equals && cond_type != PredicateCondition::NotEquals &&
      cond_type != PredicateCondition::LessThan && cond_type != PredicateCondition::LessThanEquals &&
      cond_type != PredicateCondition::GreaterThan && cond_type != PredicateCondition::GreaterThanEquals) {
    return false;
  }

  // Check whether the expression is correlated. If we would support predicates using sub-selects, we would need to
  // check the sub-selects LQP as well.
  const auto& binary_predicate_expression = std::static_pointer_cast<BinaryPredicateExpression>(predicate_expression);
  auto contains_sub_select = false;
  auto uses_correlated_parameter = false;
  for (const auto& operand :
       {binary_predicate_expression->left_operand(), binary_predicate_expression->right_operand()}) {
    if (operand->type == ExpressionType::Parameter) {
      const auto& parameter_expression = std::static_pointer_cast<ParameterExpression>(operand);
      uses_correlated_parameter |= is_correlated_parameter(parameter_expression->parameter_id);
    } else if (operand->type == ExpressionType::LQPSelect) {
      contains_sub_select = true;
    }
  }

  return uses_correlated_parameter && !contains_sub_select;
}

/**
 * Finds predicate nodes to pull up and projection nodes to remove.
 *
 * This selects all predicates that can be safely pulled up and turned into join predicates. It also collects all
 * projection nodes above the selected predicates. These need to be removed to make sure that the required columns are
 * available for the pulled up predicates.
 */
template <class ParameterPredicate>
std::pair<std::set<std::shared_ptr<PredicateNode>>, std::vector<std::shared_ptr<ProjectionNode>>>
prepare_predicate_pull_up(const std::shared_ptr<AbstractLQPNode>& lqp, ParameterPredicate&& is_correlated_parameter) {
  // We are only interested in predicate, projection, validate and sort nodes. These only ever have one input, thus we
  // can scan the path of nodes linearly. This makes it easy to track which projection nodes actually need to be
  // removed.
  std::set<std::shared_ptr<PredicateNode>> predicates_to_pull_up;
  std::vector<std::shared_ptr<ProjectionNode>> projections_found;
  size_t num_projections_to_remove = 0;

  auto node = lqp;
  while (node != nullptr) {
    if (node->type == LQPNodeType::Projection) {
      projections_found.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    } else if (node->type == LQPNodeType::Predicate) {
      const auto& predicate_node = std::static_pointer_cast<PredicateNode>(node);
      if (should_become_join_predicate(predicate_node, is_correlated_parameter)) {
        predicates_to_pull_up.emplace(predicate_node);
        // All projections found so far need to be removed
        num_projections_to_remove = projections_found.size();
      }
    } else if (node->type != LQPNodeType::Validate && node->type != LQPNodeType::Sort) {
      // It is not safe to pull up predicates passed this node, stop scanning
      break;
    }

    DebugAssert(!node->right_input(), "Scan only implemented for nodes with one input");
    node = node->left_input();
  }

  // Remove projections found below the last predicate which we don't need to remove.
  projections_found.resize(num_projections_to_remove);
  return {std::move(predicates_to_pull_up), std::move(projections_found)};
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
    const std::set<std::shared_ptr<PredicateNode>> safe_predicates) {
  bool optimizable = true;
  visit_lqp(lqp, [&](const auto& node) {
    if (!optimizable) {
      return LQPVisitation::DoNotVisitInputs;
    }

    auto correlated = uses_correlated_parameters(node, is_correlated_parameter);
    if (correlated) {
      if (node->type != LQPNodeType::Predicate ||
          safe_predicates.find(std::static_pointer_cast<PredicateNode>(node)) == safe_predicates.end()) {
        optimizable = false;
        return LQPVisitation::DoNotVisitInputs;
      }
    }

    return LQPVisitation::VisitInputs;
  });

  return !optimizable;
}

/**
 * Patches all occurrences of correlated parameters in a predicate with the parameters actual expression.
 */
void patch_correlated_predicate(
    std::shared_ptr<AbstractExpression>& join_predicate,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& correlated_parameters) {
  // Note: This does not currently handle parameter usages in sub-selects, because we don't turn predicates with
  // sub-selects into join predicates.
  visit_expression(join_predicate, [&](auto& sub_expression) {
    if (sub_expression->type == ExpressionType::Parameter) {
      const auto parameter_expression = std::static_pointer_cast<ParameterExpression>(sub_expression);
      auto it = correlated_parameters.find(parameter_expression->parameter_id);
      if (it != correlated_parameters.end()) {
        sub_expression = it->second;
      }
    }

    return ExpressionVisitation::VisitArguments;
  });
}

/**
 * Find a join predicate that can become the primary join predicate.
 *
 * Since semi and anti joins are currently only supported by hash joins, this searches for an equals expression.
 *
 * Removes a found expression from the given vector and returns it. If no candidate is found, returns a nullptr.
 */
std::shared_ptr<AbstractExpression> find_primary_join_predicate(
    std::vector<std::shared_ptr<AbstractExpression>>& join_predicates) {
  const auto end = join_predicates.end();
  for (auto it = join_predicates.begin(); it != end; ++it) {
    const auto& expression = *it;
    if (expression->type == ExpressionType::Predicate) {
      const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(expression);
      if (predicate_expression->predicate_condition == PredicateCondition::Equals) {
        // The order of the expression doesn't matter, so we can swap the found predicate to the end before removing it
        auto last_element_it = std::prev(end);
        std::iter_swap(it, last_element_it);
        auto primary_predicate = std::move(*last_element_it);
        join_predicates.pop_back();
        return primary_predicate;
      }
    }
  }

  return nullptr;
}

std::string SubselectToJoinReformulationRule::name() const { return "Sub-select to Join Reformulation Rule"; }

bool SubselectToJoinReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (NOT)-IN or (NOT)-EXISTS predicates
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();

  std::vector<std::shared_ptr<AbstractExpression>> join_predicates;
  std::shared_ptr<LQPSelectExpression> subselect_expression;
  JoinMode join_mode;
  if (predicate_node_predicate->type == ExpressionType::Predicate) {
    // Predicate might be an (NOT) IN
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

    // Check that both the left and right join expression are column references. This could be extended, however we can
    // only support expressions that are supported by the join implementation.
    if (in_expression->value()->type != ExpressionType::LQPColumn ||
        right_join_expression->type != ExpressionType::LQPColumn) {
      return _apply_to_inputs(node);
    }

    join_predicates.emplace_back(std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::Equals, in_expression->value(), right_join_expression));
    join_mode = in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
  } else if (predicate_node_predicate->type == ExpressionType::Exists) {
    const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node_predicate);
    auto exists_sub_select = exists_expression->select();

    // TODO(anybody): According to the source for ExistsExpression this could also be a PQPSelectExpression. Could this
    // be actually be the case here?
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

  // We cannot support anti joins right now (see large comment below on multi-predicate joins for the reasons).
  if (join_mode != JoinMode::Semi) {
    return _apply_to_inputs(node);
  }

  //TODO why is estimate_plan_cost not static?
  // Do not reformulate if expected output is small.
  //  if(CostModelLogical().estimate_plan_cost(node) <= Cost{50.0f}){
  if (node->get_statistics()->row_count() < 150.0f) {
    return _apply_to_inputs(node);
  }
  std::cout << "node cost before in reformulation: " << CostModelLogical().estimate_plan_cost(node) << '\n';

  // For correlated sub-queries, we use multi-predicate semi/anti joins to join on correlated predicate found in the
  // sub-query. For (NOT) IN queries, the in value is also turned into a single equals join predicate.
  //
  // To pull up predicates safely, we need to remove any projections we pull them past, to ensure that the columns
  // they use are actually available. We also only pull predicates up over other predicates, projections, sorts and
  // validates (this could probably be extended).

  // Keep track of the root of right tree when removing projections and predicates
  auto right_tree_root = subselect_expression->lqp;

  // Map parameter IDs to their respective parameter expression
  std::map<ParameterID, std::shared_ptr<AbstractExpression>> correlated_parameters;
  for (size_t i = 0; i < subselect_expression->parameter_count(); ++i) {
    correlated_parameters.emplace(subselect_expression->parameter_ids[i],
                                  subselect_expression->parameter_expression(i));
  }

  auto is_correlated_parameter = [&](ParameterID id) {
    return correlated_parameters.find(id) != correlated_parameters.end();
  };
  const auto& [correlated_predicate_nodes, projection_nodes_to_remove] =
      prepare_predicate_pull_up(right_tree_root, is_correlated_parameter);

  if (contains_unoptimizable_correlated_parameter_usages(right_tree_root, is_correlated_parameter,
                                                         correlated_predicate_nodes)) {
    return _apply_to_inputs(node);
  }

  // The reformulation might still fail if there doesn't exist a suitable primary join predicate. Therefore we need to
  // extract the join predicates first without altering the existing LQP.
  for (const auto& correlated_predicate_node : correlated_predicate_nodes) {
    auto predicate_expression = correlated_predicate_node->predicate()->deep_copy();
    patch_correlated_predicate(predicate_expression, correlated_parameters);
    join_predicates.emplace_back(std::move(predicate_expression));
  }

  const auto& primary_join_predicate = find_primary_join_predicate(join_predicates);
  if (!primary_join_predicate) {
    return _apply_to_inputs(node);
  }

  // Begin altering the LQP. All failure checks need to be finished at this point.
  for (const auto& projection_node : projection_nodes_to_remove) {
    right_tree_root = remove_from_tree(right_tree_root, projection_node);
  }

  for (const auto& correlated_predicate_node : correlated_predicate_nodes) {
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
  const auto join_node = JoinNode::make(JoinMode::Inner, primary_join_predicate);

  lqp_replace_node(node, distinct_node);
  lqp_insert_node(distinct_node, LQPInputSide::Left, left_only_projection_node);

  std::shared_ptr<AbstractLQPNode> parent = left_only_projection_node;
  for (const auto& secondary_join_predicate : join_predicates) {
    const auto secondary_predicate_node = PredicateNode::make(secondary_join_predicate);
    lqp_insert_node(parent, LQPInputSide::Left, secondary_predicate_node);
    parent = secondary_predicate_node;
  }

  lqp_insert_node(parent, LQPInputSide::Left, join_node);
  join_node->set_right_input(right_tree_root);

  std::cout << "node cost after in reformulation: " << CostModelLogical().estimate_plan_cost(distinct_node) << '\n';

  return _apply_to_inputs(distinct_node);
}

}  // namespace opossum
