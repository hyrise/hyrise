#include "exists_expression.hpp"

#include <sstream>

#include "expression/evaluation/expression_evaluator.hpp"
#include "lqp_subquery_expression.hpp"

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<AbstractExpression>& subquery,
                                   const ExistsExpressionType exists_expression_type)
    : AbstractExpression(ExpressionType::Exists, {subquery}), exists_expression_type(exists_expression_type) {
  Assert(subquery->type == ExpressionType::LQPSubquery || subquery->type == ExpressionType::PQPSubquery,
         "EXISTS needs SubqueryExpression as argument");
}

std::shared_ptr<AbstractExpression> ExistsExpression::subquery() const {
  Assert(arguments[0]->type == ExpressionType::LQPSubquery || arguments[0]->type == ExpressionType::PQPSubquery,
         "Expected to contain SubqueryExpression");
  return arguments[0];
}

std::string ExistsExpression::as_column_name() const {
  std::stringstream stream;
  stream << (exists_expression_type == ExistsExpressionType::Exists ? "EXISTS" : "NOT EXISTS");
  stream << "(" << subquery()->as_column_name() << ")";
  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(subquery()->deep_copy(), exists_expression_type);
}

DataType ExistsExpression::data_type() const { return ExpressionEvaluator::DataTypeBool; }

bool ExistsExpression::_shallow_equals(const AbstractExpression& expression) const { return true; }

size_t ExistsExpression::_on_hash() const { return AbstractExpression::_on_hash(); }

bool ExistsExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const { return false; }

}  // namespace opossum
