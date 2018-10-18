#include "exists_reformulation_rule.hpp"

#include <unordered_map>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ExistsReformulationRule::name() const { return "(Non)Exists to Join Reformulation Rule"; }

bool ExistsReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Find a PredicateNode with an EXISTS(...) predicate
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  if (!predicate_node || predicate_node->predicate->type != ExpressionType::Exists) {
    return _apply_to_inputs(node);
  }

  // Get the subselect that we work on
  const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node->predicate);
  const auto subselect_expression = std::static_pointer_cast<LQPSelectExpression>(exists_expression->select());

  // We don't care about uncorrelated subselects, nor subselects with more than one parameter
  if (subselect_expression->arguments.size() != 1) {
    return _apply_to_inputs(node);
  }

  const auto correlated_parameter_id = subselect_expression->parameter_ids[0];

  // First pass over the subselect LQP
  // Check whether the one correlated parameter is only used exactly in ONE PredicateNode. If it is used more often
  // we cannot turn the PredicateNode's predicate into a join predicate.
  auto correlated_parameter_usage_count = 0;

  visit_lqp(subselect_expression->lqp, [&](const auto& deeper_node) {
    for (const auto& expression : deeper_node->node_expressions()) {
      visit_expression(expression, [&](const auto& sub_expression) {
        const auto parameter_expression = std::dynamic_pointer_cast<ParameterExpression>(sub_expression);
        if (parameter_expression && parameter_expression->parameter_id == correlated_parameter_id) {
          ++correlated_parameter_usage_count;
        }
        return ExpressionVisitation::VisitArguments;
      });

      // Early out
      if (correlated_parameter_usage_count > 1) {
        return LQPVisitation::DoNotVisitInputs;
      }
    }
    return LQPVisitation::VisitInputs;
  });

  if (correlated_parameter_usage_count != 1) {
    return _apply_to_inputs(node);
  }

  // Second pass over the subselect LQP
  // Extract the join predicate and check whether the subselect LQP is simple enough for us to comfortably turn it into
  // a join.
  auto join_predicate = std::shared_ptr<AbstractExpression>();
  auto subselect_predicate_node = std::shared_ptr<PredicateNode>();

  visit_lqp(subselect_expression->lqp, [&](const auto& subselect_node) {
    // Play it safe. We do not know how to handle complicated LQPs (that include Joins/Unions) here
    // TODO(anybody): Projection/AliasNodes are only a problem since they might prune the column we want to join with
    //                That's fixable problem, however.
    if (subselect_node->type != LQPNodeType::Predicate && subselect_node->type != LQPNodeType::Validate &&
        subselect_node->type != LQPNodeType::StoredTable && subselect_node->type != LQPNodeType::Sort) {
      return LQPVisitation::DoNotVisitInputs;
    }

    // Skip over nodes until we find a predicate node that we could potentially extract a join predicate from
    subselect_predicate_node = std::dynamic_pointer_cast<PredicateNode>(subselect_node);
    if (!subselect_predicate_node) {
      return LQPVisitation::VisitInputs;
    }

    const auto subselect_predicate_expression =
        std::dynamic_pointer_cast<BinaryPredicateExpression>(subselect_predicate_node->predicate);
    if (!subselect_predicate_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Semi/Anti Joins are currently only implemented by the hash join, which only supports equal predicates
    if (subselect_predicate_expression->predicate_condition != PredicateCondition::Equals) {
      return LQPVisitation::VisitInputs;
    }

    // Now check if one side of the predicate is a column "outside" the subselect and the other inside
    auto inner_column_expression = std::shared_ptr<LQPColumnExpression>();
    auto parameter_expression = std::shared_ptr<ParameterExpression>();

    if (subselect_predicate_expression->arguments[0]->type == ExpressionType::LQPColumn &&
        subselect_predicate_expression->arguments[1]->type == ExpressionType::Parameter) {
      // Column left, parameter right
      inner_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(subselect_predicate_expression->arguments[0]);
      parameter_expression =
          std::static_pointer_cast<ParameterExpression>(subselect_predicate_expression->arguments[1]);
    } else if (subselect_predicate_expression->arguments[0]->type == ExpressionType::Parameter &&
               subselect_predicate_expression->arguments[1]->type == ExpressionType::LQPColumn) {
      // Column right, parameter left
      parameter_expression =
          std::static_pointer_cast<ParameterExpression>(subselect_predicate_expression->arguments[0]);
      inner_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(subselect_predicate_expression->arguments[1]);
    } else {
      // It's not - let's check the next predicate
      return LQPVisitation::VisitInputs;
    }

    if (parameter_expression->parameter_id != correlated_parameter_id) {
      // Close, but not close enough. This is a parameter of the prepared statement type
      return LQPVisitation::VisitInputs;
    }

    // Build the join predicate
    join_predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::Equals, subselect_expression->arguments[0], inner_column_expression);

    // Join predicate found, we can stop
    return LQPVisitation::DoNotVisitInputs;
  });

  if (!join_predicate) {
    // We failed to identify the join predicate or there is more than one predicate
    return _apply_to_inputs(node);
  }

  // Remove the predicate from the subselect (because it is now handled by the join) - if it is the top level node,
  // we need to remove it by pointing the LQP to its input
  if (subselect_expression->lqp == subselect_predicate_node) {
    subselect_expression->lqp = subselect_predicate_node->left_input();
  } else {
    lqp_remove_node(subselect_predicate_node);
  }

  // Build the join node and put it into the LQP in the place of the predicate
  const auto join_mode =
      exists_expression->exists_expression_type == ExistsExpressionType::Exists ? JoinMode::Semi : JoinMode::Anti;
  const auto join_node = JoinNode::make(join_mode, join_predicate);
  lqp_replace_node(predicate_node, join_node);
  join_node->set_right_input(subselect_expression->lqp);

  return true;
}

}  // namespace opossum
