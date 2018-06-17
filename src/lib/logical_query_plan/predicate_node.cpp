#include "predicate_node.hpp"

#include <memory>
#include <optional>
#include <sstream>
#include <string>

#include "constant_mappings.hpp"
#include "expression/expression_utils.hpp"
#include "expression/between_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/parameter_expression.hpp"
#include "expression/value_expression.hpp"
#include "statistics/table_statistics.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

PredicateNode::PredicateNode(const std::shared_ptr<AbstractExpression>& predicate): AbstractLQPNode(LQPNodeType::Predicate), predicate(predicate) {}

std::string PredicateNode::description() const {
  std::stringstream stream;
  stream << "[Predicate] " << predicate->as_column_name();
  return stream.str();
}

std::shared_ptr<TableStatistics> PredicateNode::derive_statistics_from(
const std::shared_ptr<AbstractLQPNode>& left_input, const std::shared_ptr<AbstractLQPNode>& right_input) const {
  DebugAssert(left_input && !right_input, "PredicateNode need left_input and no right_input");

  /**
   * If the predicate is a not simple `<column> <predicate_condition> <value>` predicate, then we have to
   * fall back to a selectivity of 1 atm, because computing statistics for complex predicates is
   * not implemented
   */

  if (predicate->type != ExpressionType::Predicate) return left_input->get_statistics();

  const auto predicate_expression = std::dynamic_pointer_cast<AbstractPredicateExpression>(predicate);
  if (!predicate_expression) return left_input->get_statistics();

  const auto binary_predicate = std::dynamic_pointer_cast<BinaryPredicateExpression>(predicate);
  const auto between_predicate = std::dynamic_pointer_cast<BetweenExpression>(predicate);
  if (!binary_predicate && !between_predicate) return left_input->get_statistics();

  /**
   * Check whether the left operand is a Column
   */
  const auto left_column = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->arguments[0]);
  if (!left_column) return left_input->get_statistics();
  const auto column_id = left_input->get_column_id(*left_column);

  /**
   * Check whether the "value" operand is a Column or a Value or a ValuePlaceholder
   */
  AllParameterVariant value;

  const auto value_value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[1]);
  const auto value_column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(predicate->arguments[1]);
  const auto value_parameter_expression = std::dynamic_pointer_cast<ParameterExpression>(predicate->arguments[1]);

  if (value_value_expression) {
    value = value_value_expression->value;
  } else if (value_column_expression) {
    value = value_column_expression->column_reference;
  } else if (value_parameter_expression && value_parameter_expression->parameter_expression_type == ParameterExpressionType::ValuePlaceholder) {
    value = value_parameter_expression->parameter_id;
  } else {
    return left_input->get_statistics();
  }

  /**
   * Check whether the "value2" operand is a Value
   */
  std::optional<AllTypeVariant> value2;

  if (between_predicate) {
    const auto value2_value_expression = std::dynamic_pointer_cast<ValueExpression>(predicate->arguments[2]);
    if (value2_value_expression) value2 = value2_value_expression->value;
  }

  return std::make_shared<TableStatistics>(left_input->get_statistics()->estimate_predicate(
  column_id, predicate_expression->predicate_condition, value, value2));
}

std::shared_ptr<AbstractLQPNode> PredicateNode::_shallow_copy_impl(LQPNodeMapping & node_mapping) const {
  return std::make_shared<PredicateNode>(expression_copy_and_adapt_to_different_lqp(*predicate, node_mapping));
}

bool PredicateNode::_shallow_equals_impl(const AbstractLQPNode& rhs, const LQPNodeMapping & node_mapping) const {
  const auto& predicate_node = static_cast<const PredicateNode&>(rhs);
  return expression_equal_to_expression_in_different_lqp(*predicate, *predicate_node.predicate, node_mapping);
}

}  // namespace opossum
