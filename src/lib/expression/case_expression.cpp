#include "case_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"

namespace hyrise {

CaseExpression::CaseExpression(const std::shared_ptr<AbstractExpression>& when,
                               const std::shared_ptr<AbstractExpression>& then,
                               const std::shared_ptr<AbstractExpression>& otherwise)
    : AbstractExpression(ExpressionType ::Case, {when, then, otherwise}) {}

const std::shared_ptr<AbstractExpression>& CaseExpression::when() const {
  return arguments[0];
}

const std::shared_ptr<AbstractExpression>& CaseExpression::then() const {
  return arguments[1];
}

const std::shared_ptr<AbstractExpression>& CaseExpression::otherwise() const {
  return arguments[2];
}

std::string CaseExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  stream << "CASE WHEN " << when()->description(mode) << " THEN " << then()->description(mode) << " ELSE "
         << otherwise()->description(mode) << " END";

  return stream.str();
}

DataType CaseExpression::data_type() const {
  return expression_common_type(then()->data_type(), otherwise()->data_type());
}

std::shared_ptr<AbstractExpression> CaseExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<CaseExpression>(when()->deep_copy(copied_ops), then()->deep_copy(copied_ops),
                                          otherwise()->deep_copy(copied_ops));
}

bool CaseExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const CaseExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return true;
}

}  // namespace hyrise
