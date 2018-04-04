#include "array_expression.hpp"

namespace opossum {

  ArrayExpression::ArrayExpression(const std::vector<std::shared_ptr<AbstractExpression>>& values):
  AbstractExpression(ExpressionType::Array, values){

  }

  const std::vector<std::shared_ptr<AbstractExpression>>& ArrayExpression::values() const {
    return arguments;
  }

  std::shared_ptr<AbstractExpression> ArrayExpression::deep_copy() const {
      return std::make_shared<ArrayExpression>(deep_copy_expressions(arguments));
  }

  std::string ArrayExpression::description() const {
    Fail("Notyetimplemented");
    return "";
  }

}  // namespace opossum
