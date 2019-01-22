#include "in_reformulation_rule.hpp"

#include <map>
#include <memory>
#include <set>

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
#include "utils/assert.hpp"

namespace opossum {

// Remove a node from an LQP tree while keeping track of its root. Returns the new root
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

// Checks whether a LQP node uses one of the correlated parameters
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

      if (sub_expression->type != ExpressionType::Parameter) {
        return ExpressionVisitation::VisitArguments;
      }

      const auto& parameter_expression = std::static_pointer_cast<ParameterExpression>(sub_expression);
      if (is_correlated_parameter(parameter_expression->parameter_id)) {
        is_correlated = true;
        return ExpressionVisitation::DoNotVisitArguments;
      }

      return ExpressionVisitation::VisitArguments;
    });

    if (is_correlated) {
      break;
    }
  }

  return is_correlated;
}

// Finds predicate nodes that need to be pulled up because they use correlated parameters. Also finds projection nodes
// that need to be removed to guarantee that the pulled predicates have access to the required columns.
template <class ParameterPredicate>
std::pair<std::set<std::shared_ptr<PredicateNode>>, std::vector<std::shared_ptr<ProjectionNode>>>
prepare_predicate_pull_up(const std::shared_ptr<AbstractLQPNode>& lqp, ParameterPredicate&& is_correlated_parameter) {
  std::set<std::shared_ptr<PredicateNode>> predicates_to_pull_up;
  std::vector<std::shared_ptr<ProjectionNode>> projections_found;
  size_t num_projections_to_remove = 0;

  // We are only interested in predicate, projection, validate and sort nodes. These only ever one input, thus we can
  // scan the path of nodes linearly. This makes it easy to track which projection nodes actually need to be removed.
  auto node = lqp;
  while (node != nullptr) {
    if (node->type == LQPNodeType::Projection) {
      projections_found.emplace_back(std::static_pointer_cast<ProjectionNode>(node));
    } else if (node->type == LQPNodeType::Predicate) {
      if (uses_correlated_parameters(node, is_correlated_parameter)) {
        predicates_to_pull_up.emplace(std::static_pointer_cast<PredicateNode>(node));
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

// Searches for usages of correlated parameters that we cannot optimize.
//
// This includes two things:
//   - Usages of correlated parameters outside of predicate nodes (for example joins)
//   - Usages of correlated parameters in predicate nodes nested below nodes they cannot be pulled up past.
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

  if (subselect_expression->arguments.empty()) {
    // For uncorrelated sub-queries, replace the in-expression by a semi/anti join using the join expressions found
    // above.
    auto join_predicate = std::make_shared<BinaryPredicateExpression>(PredicateCondition::Equals,
                                                                      in_expression->value(), right_join_expression);
    const auto join_mode = in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
    const auto join_node = JoinNode::make(join_mode, join_predicate);
    lqp_replace_node(predicate_node, join_node);
    join_node->set_right_input(subselect_expression->lqp);

    return _apply_to_inputs(join_node);
  } else {
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

    for (const auto& projection_node : projection_nodes_to_remove) {
      right_tree_root = remove_from_tree(right_tree_root, projection_node);
    }

    for (const auto& correlated_predicate_node : correlated_predicate_nodes) {
      right_tree_root = remove_from_tree(right_tree_root, correlated_predicate_node);

      // Replace placeholder expressions for correlated parameters with their actual expressions
      for (auto& expression : correlated_predicate_node->node_expressions) {
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

    // Build up replacement LQP described above
    const auto left_columns = predicate_node->left_input()->column_expressions();
    auto distinct_node = AggregateNode::make(left_columns, std::vector<std::shared_ptr<AbstractExpression>>{});
    auto left_only_projection_node = ProjectionNode::make(left_columns);
    auto join_condition = in_expression->is_negated() ? PredicateCondition::NotEquals : PredicateCondition::Equals;
    auto join_predicate =
        std::make_shared<BinaryPredicateExpression>(join_condition, in_expression->value(), right_join_expression);
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

    return _apply_to_inputs(distinct_node);
  }
}

}  // namespace opossum
