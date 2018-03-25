#include "null_expression.hpp"

#include <sstream>

namespace opossum {

NullExpression::NullExpression(): AbstractExpression(ExpressionType::Null) {

}

std::shared_ptr<AbstractExpression> NullExpression::deep_copy() const {
  return std::make_shared<NullExpression>();
}

std::string NullExpression::description() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

}  // namespace opossum
