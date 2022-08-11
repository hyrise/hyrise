#include "exists_expression.hpp"

#include <sstream>

#include "expression/evaluation/expression_evaluator.hpp"
#include "lqp_subquery_expression.hpp"

namespace hyrise {

ExistsExpression::ExistsExpression(const std::shared_ptr<AbstractExpression>& subquery,
                                   const ExistsExpressionType init_exists_expression_type)
    : AbstractExpression(ExpressionType::Exists, {subquery}), exists_expression_type(init_exists_expression_type) {
  Assert(subquery->type == ExpressionType::LQPSubquery || subquery->type == ExpressionType::PQPSubquery,
         "EXISTS needs SubqueryExpression as argument");
}

std::shared_ptr<AbstractExpression> ExistsExpression::subquery() const {
  Assert(arguments[0]->type == ExpressionType::LQPSubquery || arguments[0]->type == ExpressionType::PQPSubquery,
         "Expected to contain SubqueryExpression");
  return arguments[0];
}

std::string ExistsExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << (exists_expression_type == ExistsExpressionType::Exists ? "EXISTS" : "NOT EXISTS");
  stream << "(" << subquery()->description(mode) << ")";
  return stream.str();
}

std::shared_ptr<AbstractExpression> ExistsExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<ExistsExpression>(subquery()->deep_copy(copied_ops), exists_expression_type);
}

DataType ExistsExpression::data_type() const {
  return ExpressionEvaluator::DataTypeBool;
}

bool ExistsExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ExistsExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other_exists_expression = static_cast<const ExistsExpression&>(expression);
  return exists_expression_type == other_exists_expression.exists_expression_type;
}

size_t ExistsExpression::_shallow_hash() const {
  return exists_expression_type == ExistsExpressionType::Exists;
}

bool ExistsExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  return false;
}

}  // namespace hyrise
