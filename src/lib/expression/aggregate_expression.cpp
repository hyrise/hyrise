#include "aggregate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "utils/assert.hpp"
#include "constant_mappings.hpp"
#include "expression_utils.hpp"

namespace opossum {

AggregateExpression::AggregateExpression(const AggregateFunction aggregate_function):
  AbstractExpression(ExpressionType::Aggregate, {}) {

}

AggregateExpression::AggregateExpression(const AggregateFunction aggregate_function,
                                         const std::shared_ptr<AbstractExpression>& argument):
  AbstractExpression(ExpressionType::Aggregate, {argument}), aggregate_function(aggregate_function) {}

std::shared_ptr<AbstractExpression> AggregateExpression::argument() const {
  return arguments.empty() ? nullptr : arguments[0];
}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_copy() const {
  if (argument()) {
    return std::make_shared<AggregateExpression>(aggregate_function, argument()->deep_copy());
  } else {
    return std::make_shared<AggregateExpression>(aggregate_function, nullptr);
  }
}

std::string AggregateExpression::as_column_name() const {
  std::stringstream stream;

  stream << aggregate_function_to_string.left.at(aggregate_function) << "(";
  if (argument()) stream << argument()->as_column_name();
  stream << ")";

  return stream.str();
}

DataType AggregateExpression::data_type() const {
  // TODO(anybody) picking Long, but Int might be sufficient. How do we handle this?
  if (aggregate_function == AggregateFunction::CountDistinct || aggregate_function == AggregateFunction::Count) return DataType::Long;

  Assert(arguments.size() == 1, "Expected AggregateFunction to have one argument");

  return arguments[0]->data_type();
}

bool AggregateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return aggregate_function == static_cast<const AggregateExpression&>(expression).aggregate_function;
}

size_t AggregateExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(aggregate_function));
}

}  // namespace opossum
