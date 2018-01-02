#include "operator_expression.hpp"

#include "constant_mappings.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<OperatorExpression> OperatorExpression::create_column(const ColumnID column_id,
                                                                      const std::optional<std::string>& alias) {
  auto expression = std::make_shared<OperatorExpression>(ExpressionType::Column);
  expression->_column_id = column_id;
  expression->_alias = alias;

  return expression;
}

OperatorExpression::OperatorExpression(const std::shared_ptr<LQPExpression>& lqp_expression,
                                       const std::shared_ptr<AbstractLQPNode>& node)
    : Expression<OperatorExpression>(lqp_expression->_type) {
  _value = lqp_expression->_value;
  _aggregate_function = lqp_expression->_aggregate_function;
  _table_name = lqp_expression->_table_name;
  _value_placeholder = lqp_expression->_value_placeholder;

  if (lqp_expression->type() == ExpressionType::Column) {
    _column_id = node->get_output_column_id_by_column_origin(lqp_expression->column_origin());
  }

  for (auto& aggregate_function_argument : lqp_expression->_aggregate_function_arguments) {
    _aggregate_function_arguments.emplace_back(std::make_shared<OperatorExpression>(aggregate_function_argument, node));
  }

  if (lqp_expression->left_child()) {
    _left_child = std::make_shared<OperatorExpression>(lqp_expression->left_child(), node);
  }
  if (lqp_expression->right_child()) {
    _right_child = std::make_shared<OperatorExpression>(lqp_expression->right_child(), node);
  }

  _alias = lqp_expression->_alias;
}

ColumnID OperatorExpression::column_id() const {
  DebugAssert(_column_id, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnID");
  return *_column_id;
}

std::string OperatorExpression::to_string(const std::optional<std::vector<std::string>>& input_column_names,
                                          bool is_root) const {
  if (type() == ExpressionType::Column) {
    if (input_column_names) {
      DebugAssert(column_id() < input_column_names->size(),
                  std::string("_column_id ") + std::to_string(column_id()) + " out of range");
      return (*input_column_names)[column_id()];
    }
    return std::string("ColumnID #" + std::to_string(column_id()));
  }
  return Expression<OperatorExpression>::to_string(input_column_names, is_root);
}

bool OperatorExpression::operator==(const OperatorExpression& other) const {
  if (!Expression<OperatorExpression>::operator==(other)) {
    return false;
  }
  return _column_id == other._column_id;
}

void OperatorExpression::_deep_copy_impl(const std::shared_ptr<OperatorExpression> &copy) const {
  copy->_column_id = _column_id;
}

}
