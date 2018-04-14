#include "lqp_column_expression.hpp"

#include "utils/assert.hpp"

#include "boost/functional/hash.hpp"

namespace opossum {

LQPColumnExpression::LQPColumnExpression(const LQPColumnReference& column_reference):
  column_reference(column_reference) {}

std::shared_ptr<AbstractExpression> LQPColumnExpression::deep_copy() const {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::string LQPColumnExpression::as_column_name() const {
  Fail("TODO");
}

bool LQPColumnExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& lqp_column_expression = static_cast<const LQPColumnExpression&>(expression);
  return column_reference == lqp_column_expression.column_reference;
}

size_t LQPColumnExpression::_on_hash() const {
  return std::hash<LQPColumnReference>{}(column_reference);
}

}  // namespace opossum
