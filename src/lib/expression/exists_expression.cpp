#include "exists_expression.hpp"

#include "select_expression.hpp"

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<SelectExpression>& select):
  AbstractExpression(ExpressionType::Exists, {select}) {

}

const std::shared_ptr<SelectExpression>& ExistsExpression::select() const {
  Assert(arguments[0]->type == ExpressionType::Select, "Expected to contains Select Expression");
  return std::static_pointer_cast<SelectExpression>(arguments[0]);
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(select());
}

}  // namespace opossum
