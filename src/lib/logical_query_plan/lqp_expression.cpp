#include "lqp_expression.hpp"

#include "column_origin.hpp"
#include "utils/assert.hpp"
#include "constant_mappings.hpp"

namespace opossum {

std::shared_ptr<LQPExpression> LQPExpression::create_column(const ColumnOrigin& column_origin,
                                                      const std::optional<std::string>& alias) {
  auto expression = std::make_shared<LQPExpression>(ExpressionType::Column);
  expression->_column_origin = column_origin;
  expression->_alias = alias;

  return expression;
}

const ColumnOrigin& LQPExpression::column_origin() const {
  DebugAssert(_column_origin, "Expression " + expression_type_to_string.at(_type) + " does not have a ColumnOrigin");
  return *_column_origin;
}

std::string LQPExpression::to_string(const std::optional<std::vector<std::string>>& input_column_names) const {
  if (type() == ExpressionType::Column) {
    return column_origin().get_verbose_name();
  }
  return Expression::to_string(input_column_names);
}

}