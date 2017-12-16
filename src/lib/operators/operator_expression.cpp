#include "operator_expression.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "utils/assert.hpp"
#include "constant_mappings.hpp"

namespace opossum {

std::shared_ptr<OperatorExpression> OperatorExpression::create_column(const ColumnID column_id,
                                                      const std::optional<std::string>& alias) {
  auto expression = std::make_shared<OperatorExpression>(ExpressionType::Column);
  expression->_column_id = column_id;
  expression->_alias = alias;

  return expression;
}

OperatorExpression::OperatorExpression(const std::shared_ptr<LQPExpression>& lqp_expression,
                                       const std::shared_ptr<AbstractLQPNode>& node):
  Expression(*lqp_expression)
{
  if (lqp_expression->type() == ExpressionType::Column) {
    _column_id = node->get_output_column_id_by_column_origin(lqp_expression->column_origin());
  }
}

ColumnID OperatorExpression::column_id() const {
  DebugAssert(_column_id, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnID");
  return *_column_id;
}

std::string OperatorExpression::to_string(const std::optional<std::vector<std::string>>& input_column_names) const {
  if (type() == ExpressionType::Column) {
    if (!input_column_names) {
      DebugAssert(column_id() < input_column_names.size(),
                  std::string("_column_id ") + std::to_string(column_id()) + " out of range");
      return input_column_names[column_id()];
    }
    return std::string("ColumnID #" + std::to_string(column_id()));
  }
  return Expression::to_string(input_column_names);
}

}

