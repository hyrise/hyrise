#include "exists_expression.hpp"

#include <sstream>

#include "expression/evaluation/expression_evaluator.hpp"
#include "lqp_sub_query_expression.hpp"

namespace opossum {

ExistsExpression::ExistsExpression(const std::shared_ptr<AbstractExpression>& sub_query,
                                   const ExistsExpressionType exists_expression_type)
    : AbstractExpression(ExpressionType::Exists, {sub_query}), exists_expression_type(exists_expression_type) {
  Assert(sub_query->type == ExpressionType::LQPSubQuery || sub_query->type == ExpressionType::PQPSubQuery,
         "EXISTS needs SelectExpression as argument");
}

std::shared_ptr<AbstractExpression> ExistsExpression::sub_query() const {
  Assert(arguments[0]->type == ExpressionType::LQPSubQuery || arguments[0]->type == ExpressionType::PQPSubQuery,
         "Expected to contain SelectExpression");
  return arguments[0];
}

std::string ExistsExpression::as_column_name() const {
  std::stringstream stream;
  stream << "EXISTS(" << sub_query()->as_column_name() << ")";
  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::deep_copy() const {
  return std::make_shared<ExistsExpression>(sub_query()->deep_copy(), exists_expression_type);
}

DataType ExistsExpression::data_type() const { return ExpressionEvaluator::DataTypeBool; }

bool ExistsExpression::is_nullable() const { return false; }

bool ExistsExpression::_shallow_equals(const AbstractExpression& expression) const { return true; }

size_t ExistsExpression::_on_hash() const { return AbstractExpression::_on_hash(); }

}  // namespace opossum
