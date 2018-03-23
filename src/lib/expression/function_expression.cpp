#include "function_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

FunctionExpression::FunctionExpression(const FunctionType function_type,
                                         const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
AbstractExpression(ExpressionType::Function), function_type(function_type), arguments(arguments) {}

bool FunctionExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& function_expression = static_cast<const FunctionExpression&>(expression);
  if (function_type != function_expression.function_type) return false;
  return deep_equals_expressions(arguments, function_expression.arguments);
}

std::shared_ptr<AbstractExpression> FunctionExpression::deep_copy() const {
  return std::make_shared<FunctionExpression>(function_type, deep_copy_expressions(arguments));
}

std::shared_ptr<AbstractExpression> FunctionExpression::deep_resolve_column_expressions() {
  deep_resolve_column_expressions(arguments);
  return shared_from_this();
}

}  // namespace opossum
