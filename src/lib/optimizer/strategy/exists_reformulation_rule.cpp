#include "exists_reformulation_rule.hpp"

#include <unordered_map>

#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_subquery_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

std::string ExistsReformulationRule::name() const { return "(Non)Exists to Join Reformulation Rule"; }

void ExistsReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Find a PredicateNode with an EXISTS(...) predicate
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  if (!predicate_node || predicate_node->predicate()->type != ExpressionType::Exists) {
    _apply_to_inputs(node);
    return;
  }

  // Get the subquery that we work on
  const auto exists_expression = std::static_pointer_cast<ExistsExpression>(predicate_node->predicate());
  const auto subquery_expression = std::static_pointer_cast<LQPSubqueryExpression>(exists_expression->subquery());

  // We don't care about uncorrelated subqueries, nor subqueries with more than one parameter
  if (subquery_expression->arguments.size() != 1) {
    _apply_to_inputs(node);
    return;
  }

  const auto correlated_parameter_id = subquery_expression->parameter_ids[0];

  // First pass over the subquery LQP
  // Check whether the one correlated parameter is only used exactly in ONE PredicateNode. If it is used more often
  // we cannot turn the PredicateNode's predicate into a join predicate.
  auto correlated_parameter_usage_count = 0;

  visit_lqp(subquery_expression->lqp, [&](const auto& deeper_node) {
    for (const auto& expression : deeper_node->node_expressions) {
      visit_expression(expression, [&](const auto& sub_expression) {
        const auto parameter_expression = std::dynamic_pointer_cast<CorrelatedParameterExpression>(sub_expression);
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
    _apply_to_inputs(node);
    return;
  }

  // Second pass over the subquery LQP
  // Extract the join predicate and check whether the subquery LQP is simple enough for us to comfortably turn it into
  // a join.
  auto join_predicate = std::shared_ptr<AbstractExpression>();
  auto subquery_predicate_node = std::shared_ptr<PredicateNode>();

  visit_lqp(subquery_expression->lqp, [&](const auto& subquery_node) {
    // Play it safe. We do not know how to handle complicated LQPs (that include Joins/Unions) here
    // TODO(anybody): Projection/AliasNodes are only a problem since they might prune the column we want to join with
    //                That's fixable problem, however.
    if (subquery_node->type != LQPNodeType::Predicate && subquery_node->type != LQPNodeType::Validate &&
        subquery_node->type != LQPNodeType::StoredTable && subquery_node->type != LQPNodeType::Sort) {
      return LQPVisitation::DoNotVisitInputs;
    }

    // Skip over nodes until we find a predicate node that we could potentially extract a join predicate from
    subquery_predicate_node = std::dynamic_pointer_cast<PredicateNode>(subquery_node);
    if (!subquery_predicate_node) {
      return LQPVisitation::VisitInputs;
    }

    const auto subquery_predicate_expression =
        std::dynamic_pointer_cast<BinaryPredicateExpression>(subquery_predicate_node->predicate());
    if (!subquery_predicate_expression) {
      return LQPVisitation::VisitInputs;
    }

    // Semi/Anti Joins are currently only implemented by the hash join, which only supports equal predicates
    if (subquery_predicate_expression->predicate_condition != PredicateCondition::Equals) {
      return LQPVisitation::VisitInputs;
    }

    // Now check if one side of the predicate is a column "outside" the subquery and the other inside
    auto inner_column_expression = std::shared_ptr<LQPColumnExpression>();
    auto parameter_expression = std::shared_ptr<CorrelatedParameterExpression>();

    if (subquery_predicate_expression->arguments[0]->type == ExpressionType::LQPColumn &&
        subquery_predicate_expression->arguments[1]->type == ExpressionType::CorrelatedParameter) {
      // Column left, parameter right
      inner_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(subquery_predicate_expression->arguments[0]);
      parameter_expression =
          std::static_pointer_cast<CorrelatedParameterExpression>(subquery_predicate_expression->arguments[1]);
    } else if (subquery_predicate_expression->arguments[0]->type == ExpressionType::CorrelatedParameter &&
               subquery_predicate_expression->arguments[1]->type == ExpressionType::LQPColumn) {
      // Column right, parameter left
      parameter_expression =
          std::static_pointer_cast<CorrelatedParameterExpression>(subquery_predicate_expression->arguments[0]);
      inner_column_expression =
          std::static_pointer_cast<LQPColumnExpression>(subquery_predicate_expression->arguments[1]);
    } else {
      // It's not - let's check the next predicate
      return LQPVisitation::VisitInputs;
    }

    if (parameter_expression->parameter_id != correlated_parameter_id) {
      // Close, but not close enough. This is a parameter of placeholder type
      return LQPVisitation::VisitInputs;
    }

    // Build the join predicate
    join_predicate = std::make_shared<BinaryPredicateExpression>(
        PredicateCondition::Equals, subquery_expression->arguments[0], inner_column_expression);

    // Join predicate found, we can stop
    return LQPVisitation::DoNotVisitInputs;
  });

  if (!join_predicate) {
    // We failed to identify the join predicate or there is more than one predicate
    _apply_to_inputs(node);
    return;
  }

  // Remove the predicate from the subquery (because it is now handled by the join) - if it is the top level node,
  // we need to remove it by pointing the LQP to its input
  if (subquery_expression->lqp == subquery_predicate_node) {
    subquery_expression->lqp = subquery_predicate_node->left_input();
  } else {
    lqp_remove_node(subquery_predicate_node);
  }

  // Build the join node and put it into the LQP in the place of the predicate
  const auto join_mode =
      exists_expression->exists_expression_type == ExistsExpressionType::Exists ? JoinMode::Semi : JoinMode::Anti;
  const auto join_node = JoinNode::make(join_mode, join_predicate);
  lqp_replace_node(predicate_node, join_node);
  join_node->set_right_input(subquery_expression->lqp);
}

}  // namespace opossum
