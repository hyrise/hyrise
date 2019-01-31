#include "expression_functional.hpp"

namespace opossum {

namespace expression_functional {

std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression) {
  return expression;
}

std::shared_ptr<LQPColumnExpression> to_expression(const LQPColumnReference& column_reference) {
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::shared_ptr<ValueExpression> to_expression(const AllTypeVariant& value) {
  return std::make_shared<ValueExpression>(value);
}

std::shared_ptr<ValueExpression> value_(const AllTypeVariant& value) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ValueExpression>(value);
}

// otherwise the NOLINT markers get misplaced
// clang-format off
std::shared_ptr<ValueExpression> null_() { // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ValueExpression>(NullValue{});
}

std::shared_ptr<PlaceholderExpression> placeholder_(const ParameterID parameter_id) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<PlaceholderExpression>(parameter_id);
}

std::shared_ptr<LQPColumnExpression> lqp_column_(const LQPColumnReference& column_reference) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<LQPColumnExpression>(column_reference);
}

std::shared_ptr<PQPColumnExpression> pqp_column_(const ColumnID column_id, const DataType data_type, const bool nullable,  // NOLINT - clang-tidy doesn't like the suffix
                                             const std::string& column_name) {
  return std::make_shared<PQPColumnExpression>(column_id, data_type, nullable, column_name);
}

std::shared_ptr<AggregateExpression> count_star_() {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<AggregateExpression>(AggregateFunction::Count);
}

std::shared_ptr<ExistsExpression> exists_(const std::shared_ptr<AbstractExpression>& subquery_expression) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ExistsExpression>(subquery_expression, ExistsExpressionType::Exists);
}

std::shared_ptr<ExistsExpression> not_exists_(const std::shared_ptr<AbstractExpression>& subquery_expression) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ExistsExpression>(subquery_expression, ExistsExpressionType::NotExists);
}
// clang-format on

}  // namespace expression_functional

}  // namespace opossum
