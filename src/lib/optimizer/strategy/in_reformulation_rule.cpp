#include "in_reformulation_rule.hpp"

#include <memory>

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

namespace opossum {

std::string InReformulationRule::name() const { return "(Not)In to Join Reformulation Rule"; }

bool InReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (not)in predicates
  if (node->type != LQPNodeType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_node = std::static_pointer_cast<PredicateNode>(node);
  const auto predicate = predicate_node->predicate();
  if (predicate->type != ExpressionType::Predicate) {
    return _apply_to_inputs(node);
  }

  const auto predicate_expression = std::static_pointer_cast<AbstractPredicateExpression>(predicate);
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

  // Only optimize sub-queries with up to one argument
  if (subselect_expression->arguments.size() > 1) {
    return _apply_to_inputs(node);
  }

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
  } else {
    // For correlated sub-queries, we would ideally use multi-predicate semi joins to join on the join attribute and
    // any correlated predicate found in the sub-query. Since these are still work-in-progress, we emulate them by:
    //   - Performing an inner join on the two trees
    //   - Pulling up correlated predicates (only implemented for one for now) above the join
    //   - Inserting a projection above the predicates to filter out any columns from the right sub-tree
    //   - Inserting a group by over all columns from the left subtree, to filter out duplicates introduced by the join
    //
    // NOTE: This only works correctly if the left sub-tree does not contain any duplicates. It is only meant to get
    // the implementation started and to collect some preliminary benchmark results.
    //
    // To pull up predicates safely, we need to remove any projections we pull them past, to ensure that the columns
    // they use are actually available. We also only pull predicates up over other predicates, projections, sorts and
    // validates (this could probably be extended).

    // Keep track of the root of right tree when removing projections and predicates
    auto right_tree_root = subselect_expression->lqp;
    auto correlated_predicate_node = std::shared_ptr<PredicateNode>();
    const auto correlated_parameter_id = subselect_expression->parameter_ids[0];
    auto can_optimize = true;

    // Search for usages of the correlated parameter. Return without optimizing if:
    //   - the parameter is used more than once
    //   - the parameter is used outside predicate nodes
    visit_lqp(right_tree_root, [&](const auto& deeper_node) {
      for (const auto& expression : deeper_node->node_expressions) {
        visit_expression(expression, [&](const auto& sub_expression) {
          const auto parameter_expression = std::dynamic_pointer_cast<ParameterExpression>(sub_expression);
          if (parameter_expression && parameter_expression->parameter_id == correlated_parameter_id) {
            if (deeper_node->type == LQPNodeType::Predicate && !correlated_predicate_node) {
              correlated_predicate_node = std::static_pointer_cast<PredicateNode>(deeper_node);
            } else {
              can_optimize = false;
              return ExpressionVisitation::DoNotVisitArguments;
            }
          }
          return ExpressionVisitation::VisitArguments;
        });

        // Early out
        if (!can_optimize) {
          return LQPVisitation::DoNotVisitInputs;
        }
      }
      return LQPVisitation::VisitInputs;
    });

    if (!can_optimize || !correlated_predicate_node) {
      return _apply_to_inputs(node);
    }

    // Find and remove projections over the predicate node, so that it can be pulled up safely.
    // Return early if the predicate node is nested below a node where we are not sure that we can safely pull it up
    // passed (joins, etc.).
    auto found_predicate = false;
    std::vector<std::shared_ptr<ProjectionNode>> projections_to_remove;
    visit_lqp(right_tree_root, [&](const auto& deeper_node) {
      if (deeper_node->type == LQPNodeType::Projection) {
        projections_to_remove.emplace_back(std::static_pointer_cast<ProjectionNode>(deeper_node));
        return LQPVisitation::VisitInputs;
      } else if (deeper_node->type == LQPNodeType::Predicate && deeper_node == correlated_predicate_node) {
        found_predicate = true;
        return LQPVisitation::DoNotVisitInputs;
      } else if (deeper_node->type == LQPNodeType::Predicate || deeper_node->type == LQPNodeType::Validate ||
                 deeper_node->type == LQPNodeType::StoredTable || deeper_node->type == LQPNodeType::Sort) {
        // Only walk over nodes where we are sure we can pull up the predicate safely
        return LQPVisitation::VisitInputs;
      }

      return LQPVisitation::DoNotVisitInputs;
    });

    if (!found_predicate) {
      // Predicate nested below nodes it cannot be pulled up over
      return _apply_to_inputs(node);
    }

    // Remove projections and the found predicate from the right sub-tree, while tracking the root node for it
    for (const auto& projection_node : projections_to_remove) {
      if (right_tree_root == projection_node) {
        right_tree_root = projection_node->left_input();
      }

      lqp_remove_node(projection_node);
    }

    if (right_tree_root == correlated_predicate_node) {
      right_tree_root = correlated_predicate_node->left_input();
    }

    lqp_remove_node(correlated_predicate_node);

    // Replace sub-query parameter id in the predicate node with a reference to the column from the left sub-tree
    auto correlated_parameter_expression = subselect_expression->parameter_expression(0);
    for (auto& expression : correlated_predicate_node->node_expressions) {
      visit_expression(expression, [&](auto& sub_expression) {
        if (sub_expression->type != ExpressionType::Parameter) {
          return ExpressionVisitation::VisitArguments;
        }

        const auto parameter_expression = std::static_pointer_cast<ParameterExpression>(sub_expression);
        if (parameter_expression && parameter_expression->parameter_id == correlated_parameter_id) {
          sub_expression = correlated_parameter_expression;
          return ExpressionVisitation::DoNotVisitArguments;
        }

        return ExpressionVisitation::VisitArguments;
      });
    }

    // Build the projection and join nodes and insert them in order described above
    const auto left_columns = predicate_node->left_input()->column_expressions();
    auto distinct_node = AggregateNode::make(left_columns, std::vector<std::shared_ptr<AbstractExpression>>{});
    auto left_only_projection_node = ProjectionNode::make(left_columns);
    auto join_condition = in_expression->is_negated() ? PredicateCondition::NotEquals : PredicateCondition::Equals;
    auto join_predicate =
        std::make_shared<BinaryPredicateExpression>(join_condition, in_expression->value(), right_join_expression);
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate);
    lqp_replace_node(predicate_node, distinct_node);
    lqp_insert_node(distinct_node, LQPInputSide::Left, left_only_projection_node);
    lqp_insert_node(left_only_projection_node, LQPInputSide::Left, correlated_predicate_node);
    lqp_insert_node(correlated_predicate_node, LQPInputSide::Left, join_node);
    join_node->set_right_input(right_tree_root);
  }

  return true;
}

}  // namespace opossum
