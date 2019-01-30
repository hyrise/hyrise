#include "in_reformulation_rule.hpp"

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
    const std::shared_ptr<PredicateNode>& predicate_node,
    const std::map<ParameterID, std::shared_ptr<AbstractExpression>>& correlated_parameters) {
  // Note: This does not currently handle parameter usages in sub-selects, because we don't turn predicates with
  // sub-selects into join predicates.
  for (auto& expression : predicate_node->node_expressions) {
    visit_expression(expression, [&](auto& sub_expression) {
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
}

std::string InReformulationRule::name() const { return "(Not)In to Join Reformulation Rule"; }

bool InReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (not)in predicates
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate_node_predicate = predicate_node->predicate();
  if (predicate_node_predicate->type != ExpressionType::Predicate) {
    return _apply_to_inputs(node);
  }

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

  const auto subselect_expression = std::static_pointer_cast<LQPSelectExpression>(in_expression->set());

  // Find the single column that the sub-query should produce and turn it into our join attribute.
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

  //TODO why is estimate_plan_cost not static?

  // Do not reformulate if expected output is small.
  //  if(CostModelLogical().estimate_plan_cost(node) <= Cost{50.0f}){
  if (node->get_statistics()->row_count() < 150.0f) {
    return _apply_to_inputs(node);
  }
  std::cout << "node cost before in reformulation: " << CostModelLogical().estimate_plan_cost(node) << '\n';

  if (subselect_expression->arguments.empty()) {
    // For uncorrelated sub-queries replace the in-expression with a semi/anti join using
    // the join expressions found above.
    auto join_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals,
                                                                      in_expression->value(), right_join_expression);
    const auto join_mode = in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
    const auto join_node = JoinNode::make(join_mode, join_predicate);
    lqp_replace_node(predicate_node, join_node);
    join_node->set_right_input(subselect_expression->lqp);

    return _apply_to_inputs(join_node);
  }

  // For correlated sub-queries, we use multi-predicate semi/anti joins to join on the in-value and any correlated
  // predicate found in the sub-query.
  //
  // Since multi-predicate joins are currently still work-in-progress, we emulate them by:
  //   - Performing an inner join on the two trees
  //   - Pulling up correlated predicates above the join
  //   - Inserting a projection above the predicates to filter out any columns from the right sub-tree
  //   - Inserting a group by over all columns from the left subtree, to filter out duplicates introduced by the join
  //
  // NOTE: This only works correctly if the left sub-tree does not contain any duplicates. It also works very wrong
  // for NOT IN expressions. It is only meant to get the implementation started and to collect some preliminary
  // benchmark results.
  //
  // To pull up predicates safely, we need to remove any projections we pull them past, to ensure that the columns
  // they use are actually available. We also only pull predicates up over other predicates, projections, sorts and
  // validates (this could probably be extended).

  // NOT IN is not yet supported for correlated queries there is no good way to emulate multi-predicate anti-joins
  // using with any other join type, and we cannot use single-predicate an anti-join since it discards the columns of
  // its right input, which we would need to pull up predicates.
  if (in_expression->is_negated()) {
    return _apply_to_inputs(node);
  }

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

  for (const auto& correlated_predicate_node : correlated_predicate_nodes) {
    right_tree_root = remove_from_tree(right_tree_root, correlated_predicate_node);
    patch_correlated_predicate(correlated_predicate_node, correlated_parameters);
  }

  // Build up replacement LQP as described above
  const auto left_columns = predicate_node->left_input()->column_expressions();
  auto distinct_node = AggregateNode::make(left_columns, std::vector<std::shared_ptr<AbstractExpression>>{});
  auto left_only_projection_node = ProjectionNode::make(left_columns);
  auto join_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals, in_expression->value(),
                                                                    right_join_expression);
  const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate);

  lqp_replace_node(predicate_node, distinct_node);
  lqp_insert_node(distinct_node, LQPInputSide::Left, left_only_projection_node);

  std::shared_ptr<AbstractLQPNode> parent = left_only_projection_node;
  for (const auto& correlated_predicate_node : correlated_predicate_nodes) {
    lqp_insert_node(parent, LQPInputSide::Left, correlated_predicate_node);
    parent = correlated_predicate_node;
  }

  lqp_insert_node(parent, LQPInputSide::Left, join_node);
  join_node->set_right_input(right_tree_root);

  std::cout << "node cost after in reformulation: " << CostModelLogical().estimate_plan_cost(distinct_node) << '\n';

  return _apply_to_inputs(distinct_node);
}

}  // namespace opossum

// SELECT * FROM customer WHERE c_custkey IN (SELECT o_custkey FROM orders WHERE o_totalprice > c_acctbal)
