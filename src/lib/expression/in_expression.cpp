#include "in_expression.hpp"

#include <sstream>

namespace opossum {

InExpression::InExpression(const std::shared_ptr<AbstractExpression>& value,
                           const std::shared_ptr<AbstractExpression>& set)
    : AbstractPredicateExpression(PredicateCondition::In, {value, set}) {}

const std::shared_ptr<AbstractExpression>& InExpression::value() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& InExpression::set() const { return arguments[1]; }

std::shared_ptr<AbstractExpression> InExpression::deep_copy() const {
  return std::make_shared<InExpression>(value()->deep_copy(), set()->deep_copy());
}

std::string InExpression::as_cxlumn_name() const {
  std::stringstream stream;
  stream << _enclose_argument_as_cxlumn_name(*value()) << " IN " << set()->as_cxlumn_name();
  return stream.str();
}

}  // namespace opossum
