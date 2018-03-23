#include "aggregate_expression.hpp"

#include "utils/assert.hpp"

namespace opossum {

AggregateExpression::AggregateExpression(const AggregateType aggregate_type,
                    const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  AbstractExpression(ExpressionType::Aggregate), aggregate_type(aggregate_type), arguments(arguments) {}

bool AggregateExpression::deep_equals(const AbstractExpression& expression) const {
  if (type != expression.type) return false;

  const auto& aggregate_expression = static_cast<const AggregateExpression&>(expression);
  if (aggregate_type != aggregate_expression.aggregate_type) return false;
  return deep_equals_expressions(arguments, aggregate_expression.arguments);
}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_copy() const {
  return std::make_shared<AggregateExpression>(aggregate_type, deep_copy_expressions(arguments));
}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_resolve_column_expressions() {
  deep_resolve_column_expressions(arguments);
  return shared_from_this();
}

}  // namespace opossum
