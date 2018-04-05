#include "function_expression.hpp"

#include "utils/assert.hpp"

#include <sstream>

namespace opossum {

FunctionExpression::FunctionExpression(const FunctionType function_type,
                                         const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
AbstractExpression(ExpressionType::Function, arguments) {}

std::shared_ptr<AbstractExpression> FunctionExpression::deep_copy() const {
  return std::make_shared<FunctionExpression>(function_type, deep_copy_expressions(arguments));
}

std::string FunctionExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool FunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  return function_type == static_cast<const FunctionExpression&>(expression).function_type;
}

}  // namespace opossum
