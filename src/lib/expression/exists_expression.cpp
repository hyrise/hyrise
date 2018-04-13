#include "exists_expression.hpp"

#include "lqp_select_expression.hpp"

#include <sstream>

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<LQPSelectExpression>& select):
  AbstractExpression(ExpressionType::Exists, {select}) {

}

std::shared_ptr<LQPSelectExpression> ExistsExpression::select() const {
  Assert(arguments[0]->type == ExpressionType::Select, "Expected to contains Select Expression");
  return std::static_pointer_cast<LQPSelectExpression>(arguments[0]);
}

std::string ExistsExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(select());
}

}  // namespace opossum
