#include "external_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

ExternalExpression::ExternalExpression(const std::shared_ptr<AbstractExpression> &referenced_expression):
  AbstractExpression(ExpressionType::External, {referenced_expression})
{

}

std::shared_ptr<AbstractExpression> ExternalExpression::deep_copy() const{
  return std::make_shared<ExternalExpression>(referenced_expression()->deep_copy());
}


std::string ExternalExpression::as_column_name() const{
  return referenced_expression()->as_column_name();
}

ExpressionDataTypeVariant ExternalExpression::data_type() const {
  return referenced_expression()->data_type();
}

std::shared_ptr<AbstractExpression> ExternalExpression::referenced_expression() const {
  return arguments[0];
}


}