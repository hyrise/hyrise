#include "array_expression.hpp"

#include "expression_utils.hpp"

namespace opossum {

  ArrayExpression::ArrayExpression(const std::vector<std::shared_ptr<AbstractExpression>>& values):
  AbstractExpression(ExpressionType::Array, values){

  }

  const std::vector<std::shared_ptr<AbstractExpression>>& ArrayExpression::values() const {
    return arguments;
  }

  std::shared_ptr<AbstractExpression> ArrayExpression::deep_copy() const {
      return std::make_shared<ArrayExpression>(expressions_copy(arguments));
  }

  std::string ArrayExpression::as_column_name() const {
    Fail("Notyetimplemented");
    return "";
  }

}  // namespace opossum
