#include "exists_expression.hpp"

#include <sstream>

#include "expression/evaluation/expression_evaluator.hpp"
#include "lqp_select_expression.hpp"

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<AbstractExpression>& select)
    : AbstractExpression(ExpressionType::Exists, {select}) {
  Assert(select->type == ExpressionType::LQPSelect || select->type == ExpressionType::PQPSelect,
         "EXISTS needs SelectExpression as argument");
}

std::shared_ptr<AbstractExpression> ExistsExpression::select() const {
  Assert(arguments[0]->type == ExpressionType::LQPSelect || arguments[0]->type == ExpressionType::PQPSelect,
         "Expected to contain SelectExpression");
  return arguments[0];
}

std::string ExistsExpression::as_column_name() const {
  std::stringstream stream;
  stream << "EXISTS(" << select()->as_column_name() << ")";
  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(select()->deep_copy());
}

DataType ExistsExpression::data_type() const { return ExpressionEvaluator::DataTypeBool; }

bool ExistsExpression::is_nullable() const { return false; }

bool ExistsExpression::_shallow_equals(const AbstractExpression& expression) const { return true; }

size_t ExistsExpression::_on_hash() const { return AbstractExpression::_on_hash(); }

}  // namespace opossum
