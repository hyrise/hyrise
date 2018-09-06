#include "expression_functional.hpp"

namespace opossum {

namespace expression_functional {

std::shared_ptr<AbstractExpression> to_expression(const std::shared_ptr<AbstractExpression>& expression) {
  return expression;
}

std::shared_ptr<LQPCxlumnExpression> to_expression(const LQPCxlumnReference& cxlumn_reference) {
  return std::make_shared<LQPCxlumnExpression>(cxlumn_reference);
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

std::shared_ptr<ParameterExpression> parameter_(const ParameterID parameter_id) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ParameterExpression>(parameter_id);
}

std::shared_ptr<LQPCxlumnExpression> cxlumn_(const LQPCxlumnReference& cxlumn_reference) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<LQPCxlumnExpression>(cxlumn_reference);
}

std::shared_ptr<PQPCxlumnExpression> cxlumn_(const CxlumnID cxlumn_id, const DataType data_type, const bool nullable,  // NOLINT - clang-tidy doesn't like the suffix
                                             const std::string& cxlumn_name) {
  return std::make_shared<PQPCxlumnExpression>(cxlumn_id, data_type, nullable, cxlumn_name);
}

std::shared_ptr<AggregateExpression> count_star_() {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<AggregateExpression>(AggregateFunction::Count);
}

std::shared_ptr<ExistsExpression> exists_(const std::shared_ptr<AbstractExpression>& select_expression) {  // NOLINT - clang-tidy doesn't like the suffix
  return std::make_shared<ExistsExpression>(select_expression);
}
// clang-format on

}  // namespace expression_functional

}  // namespace opossum
