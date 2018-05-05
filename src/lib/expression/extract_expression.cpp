#include "extract_expression.hpp"

namespace opossum {

ExtractExpression::ExtractExpression(const DateComponent date_component, const std::shared_ptr<AbstractExpression>& from):
  AbstractExpression(ExpressionType::Extract, {from}), date_component(date_component) {}

std::shared_ptr<AbstractExpression> ExtractExpression::deep_copy() const {
  return std::make_shared<ExtractExpression>(date_component, from()->deep_copy());
}

std::string ExtractExpression::as_column_name() const  {
  Fail("not yet implemented");
}

DataType ExtractExpression::data_type() const  {
  // Dates are Strings, DateComponents COULD be Ints, but lets leave it at String for now
  return DataType::String;
}

std::shared_ptr<AbstractExpression> ExtractExpression::from() const {
  return arguments[0];
}

}  // namespace opossum