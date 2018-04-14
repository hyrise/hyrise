#include "aggregate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "utils/assert.hpp"
#include "constant_mappings.hpp"
#include "expression_utils.hpp"

namespace opossum {

AggregateExpression::AggregateExpression(const AggregateType aggregate_type,
                    const std::vector<std::shared_ptr<AbstractExpression>>& arguments):
  AbstractExpression(ExpressionType::Aggregate, arguments), aggregate_type(aggregate_type) {}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_copy() const {
  return std::make_shared<AggregateExpression>(aggregate_type, expressions_copy(arguments));
}

std::string AggregateExpression::as_column_name() const {
  std::stringstream stream;

  Fail("Todo");

  return stream.str();
}

bool AggregateExpression::_shallow_equals(const AbstractExpression& expression) const {
  return aggregate_type == static_cast<const AggregateExpression&>(expression).aggregate_type;
}

size_t AggregateExpression::_on_hash() const {
  return boost::hash_value(static_cast<size_t>(aggregate_type));
}

}  // namespace opossum
