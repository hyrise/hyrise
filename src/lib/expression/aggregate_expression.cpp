#include "aggregate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "utils/assert.hpp"
#include "constant_mappings.hpp"
#include "expression_utils.hpp"

namespace opossum {

AggregateExpression::AggregateExpression(const AggregateFunction aggregate_function,
                    const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  AbstractExpression(ExpressionType::Aggregate, arguments), aggregate_function(aggregate_function) {}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_copy() const {
  return std::make_shared<AggregateExpression>(aggregate_function, expressions_copy(arguments));
}

std::string AggregateExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

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
