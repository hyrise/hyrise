#include "in_reformulation_rule.hpp"

#include <memory>

#include "expression/abstract_expression.hpp"
#include "expression/abstract_predicate_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"

namespace opossum {

std::string InReformulationRule::name() const { return "(Not)In to Join Reformulation Rule"; }

bool InReformulationRule::apply_to(const std::shared_ptr<AbstractLQPNode>& node) const {
  // Filter out all nodes that are not (not)in predicates
  const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node);
  if (!predicate_node) {
    return _apply_to_inputs(node);
  }

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

  // Do not optimize cases where the in value is anything other than a column (for now)
  if (in_expression->value()->type != ExpressionType::LQPColumn) {
    return _apply_to_inputs(node);
  }

  // Only optimize if the set is a sub-select, and not a static list
  const auto subselect_expression = std::dynamic_pointer_cast<LQPSelectExpression>(in_expression->set());
  if (!subselect_expression) {
    return _apply_to_inputs(node);
  }

  // Only optimize uncorrelated sub-queries
  if (!subselect_expression->arguments.empty()) {
    return _apply_to_inputs(node);
  }

  // Find the top-most projection node to find the field that is checked
  // (which turns into our join attribute)
  auto right_join_expression = std::shared_ptr<LQPColumnExpression>();
  visit_lqp(subselect_expression->lqp, [&](const auto subselect_node) {
    // Only walk over nodes that do not change the set of columns in the result
    if (subselect_node->type != LQPNodeType::Predicate && subselect_node->type != LQPNodeType::Validate &&
        subselect_node->type != LQPNodeType::StoredTable && subselect_node->type != LQPNodeType::Sort &&
        subselect_node->type != LQPNodeType::Projection) {
      return LQPVisitation::DoNotVisitInputs;
    }

    if (subselect_node->type != LQPNodeType::Projection) {
      return LQPVisitation::VisitInputs;
    }

    // Skip cases were more than one column is selected, and do not optimize anything other
    // then simple column expressions (as opposed to aggregate functions, ...)
    auto subselect_projection_node = std::static_pointer_cast<ProjectionNode>(subselect_node);
    const auto column_expressions = subselect_projection_node->column_expressions();
    if (column_expressions.size() != 1 || column_expressions[0]->type != ExpressionType::LQPColumn) {
      return LQPVisitation::DoNotVisitInputs;
    }

    right_join_expression = std::static_pointer_cast<LQPColumnExpression>(column_expressions[0]);
    return LQPVisitation::DoNotVisitInputs;
  });

  if (!right_join_expression) {
    return _apply_to_inputs(node);
  }

  // Build join node and replace the predicate with it
  auto left_join_expression = std::static_pointer_cast<LQPColumnExpression>(in_expression->value());
  auto join_predicate = std::make_shared<BinaryPredicateExpression>(
      PredicateCondition::Equals, left_join_expression, right_join_expression);
  const auto join_mode =
      in_expression->is_negated() ? JoinMode::Anti : JoinMode::Semi;
  const auto join_node = JoinNode::make(join_mode, join_predicate);
  lqp_replace_node(predicate_node, join_node);
  join_node->set_right_input(subselect_expression->lqp);

  return true;
}

}  // namespace opossum
