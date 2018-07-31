#include "case_expression.hpp"

#include <sstream>

#include "expression_utils.hpp"

namespace opossum {

CaseExpression::CaseExpression(const std::shared_ptr<AbstractExpression>& when,
                               const std::shared_ptr<AbstractExpression>& then,
                               const std::shared_ptr<AbstractExpression>& otherwise)
    : AbstractExpression(ExpressionType ::Case, {when, then, otherwise}) {}

const std::shared_ptr<AbstractExpression>& CaseExpression::when() const { return arguments[0]; }

const std::shared_ptr<AbstractExpression>& CaseExpression::then() const { return arguments[1]; }

const std::shared_ptr<AbstractExpression>& CaseExpression::otherwise() const { return arguments[2]; }

std::string CaseExpression::as_column_name() const {
  std::stringstream stream;

  stream << "CASE WHEN " << when()->as_column_name() << " THEN " << then()->as_column_name() << " ELSE "
         << otherwise()->as_column_name() << " END";

  return stream.str();
}

DataType CaseExpression::data_type() const {
  return expression_common_type(then()->data_type(), otherwise()->data_type());
}

std::shared_ptr<AbstractExpression> CaseExpression::deep_copy() const {
  return std::make_shared<CaseExpression>(when()->deep_copy(), then()->deep_copy(), otherwise()->deep_copy());
}

bool CaseExpression::_shallow_equals(const AbstractExpression& expression) const { return true; }

size_t CaseExpression::_on_hash() const { return AbstractExpression::_on_hash(); }

}  // namespace opossum
