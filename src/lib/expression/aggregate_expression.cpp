#include "aggregate_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"

#include "constant_mappings.hpp"
#include "expression_utils.hpp"
#include "lqp_column_expression.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace opossum {

AggregateExpression::AggregateExpression(const AggregateFunction aggregate_function,
                                         const std::shared_ptr<AbstractExpression>& argument)
    : AbstractExpression(ExpressionType::Aggregate, {argument}), aggregate_function(aggregate_function) {}

std::shared_ptr<AbstractExpression> AggregateExpression::argument() const {
  return arguments.empty() ? nullptr : arguments[0];
}

std::shared_ptr<AbstractExpression> AggregateExpression::deep_copy() const {
  return std::make_shared<AggregateExpression>(aggregate_function, argument()->deep_copy());
}

std::string AggregateExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (aggregate_function == AggregateFunction::CountDistinct) {
    Assert(argument(), "COUNT(DISTINCT ...) requires an argument");
    stream << "COUNT(DISTINCT " << argument()->description(mode) << ")";
  } else if (is_count_star(*this)) {
    if (mode == DescriptionMode::ColumnName) {
      stream << "COUNT(*)";
    } else {
      const auto column_expression = dynamic_cast<const LQPColumnExpression*>(&*argument());
      DebugAssert(column_expression, "Expected aggregate argument to be column expression");
      stream << "COUNT(" << column_expression->column_reference.original_node() << ".*)";
    }
  } else {
    stream << aggregate_function << "(";
    if (argument()) stream << argument()->description(mode);
    stream << ")";
  }

  return stream.str();
}

DataType AggregateExpression::data_type() const {
  if (aggregate_function == AggregateFunction::Count) {
    return AggregateTraits<NullValue, AggregateFunction::Count>::AGGREGATE_DATA_TYPE;
  }

  Assert(arguments.size() == 1, "Expected this AggregateFunction to have one argument");

  if (aggregate_function == AggregateFunction::CountDistinct) {
    return AggregateTraits<NullValue, AggregateFunction::CountDistinct>::AGGREGATE_DATA_TYPE;
  }

  const auto argument_data_type = argument()->data_type();
  auto aggregate_data_type = DataType::Null;

  resolve_data_type(argument_data_type, [&](const auto data_type_t) {
    using AggregateDataType = typename decltype(data_type_t)::type;
    switch (aggregate_function) {
      case AggregateFunction::Min:
        aggregate_data_type = AggregateTraits<AggregateDataType, AggregateFunction::Min>::AGGREGATE_DATA_TYPE;
        break;
      case AggregateFunction::Max:
        aggregate_data_type = AggregateTraits<AggregateDataType, AggregateFunction::Max>::AGGREGATE_DATA_TYPE;
        break;
      case AggregateFunction::Avg:
        aggregate_data_type = AggregateTraits<AggregateDataType, AggregateFunction::Avg>::AGGREGATE_DATA_TYPE;
        break;
      case AggregateFunction::Count:
      case AggregateFunction::CountDistinct:
        break;  // These are handled above
      case AggregateFunction::Sum:
        aggregate_data_type = AggregateTraits<AggregateDataType, AggregateFunction::Sum>::AGGREGATE_DATA_TYPE;
        break;
      case AggregateFunction::StandardDeviationSample:
        aggregate_data_type =
            AggregateTraits<AggregateDataType, AggregateFunction::StandardDeviationSample>::AGGREGATE_DATA_TYPE;
        break;
      case AggregateFunction::Any:
        aggregate_data_type = AggregateTraits<AggregateDataType, AggregateFunction::Any>::AGGREGATE_DATA_TYPE;
        break;
    }
  });

  return aggregate_data_type;
}

bool AggregateExpression::is_count_star(const AbstractExpression& expression) {
  // COUNT(*) is represented by an AggregateExpression with the COUNT function and an INVALID_COLUMN_ID.
  if (expression.type != ExpressionType::Aggregate) return false;
  const auto& aggregate_expression = static_cast<const AggregateExpression&>(expression);

  if (aggregate_expression.aggregate_function != AggregateFunction::Count) return false;
  if (aggregate_expression.argument()->type != ExpressionType::LQPColumn) return false;

  const auto& lqp_column_expression = static_cast<LQPColumnExpression&>(*aggregate_expression.argument());
  if (lqp_column_expression.column_reference.original_column_id() != INVALID_COLUMN_ID) return false;  // NOLINT

  return true;
}

bool AggregateExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const AggregateExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return aggregate_function == static_cast<const AggregateExpression&>(expression).aggregate_function;
}

size_t AggregateExpression::_shallow_hash() const { return boost::hash_value(static_cast<size_t>(aggregate_function)); }

bool AggregateExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // Aggregates (except COUNT and COUNT DISTINCT) will return NULL when executed on an
  // empty group - thus they are always nullable
  return aggregate_function != AggregateFunction::Count && aggregate_function != AggregateFunction::CountDistinct;
}

}  // namespace opossum
