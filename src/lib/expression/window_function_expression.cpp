#include "window_function_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"
#include "lqp_column_expression.hpp"
#include "operators/aggregate/aggregate_traits.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace hyrise {

WindowFunctionExpression::WindowFunctionExpression(const WindowFunction init_aggregate_function,
                                                   const std::shared_ptr<AbstractExpression>& argument,
                                                   const std::shared_ptr<AbstractExpression>& window)
    : AbstractExpression{ExpressionType::WindowFunction, {}}, aggregate_function(init_aggregate_function) {
  arguments.reserve(2);
  if (argument) {
    arguments.emplace_back(argument);
    _has_operand = true;
  }
  if (window) {
    arguments.emplace_back(window);
  }
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::operand() const {
  return _has_operand ? arguments[0] : nullptr;
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::window() const {
  const auto expression_id = _has_operand ? size_t{1} : size_t{0};
  const auto argument_count = arguments.size();
  return expression_id < arguments.size() ? arguments[argument_count - 1] : nullptr;
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto& copied_operand = _has_operand ? operand()->deep_copy(copied_ops) : nullptr;
  const auto& window = this->window();
  const auto& copied_window = window ? window->deep_copy(copied_ops) : nullptr;
  return std::make_shared<WindowFunctionExpression>(aggregate_function, copied_operand, copied_window);
}

std::string WindowFunctionExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;

  if (aggregate_function == WindowFunction::CountDistinct) {
    Assert(operand(), "COUNT(DISTINCT ...) requires an argument");
    stream << "COUNT(DISTINCT " << operand()->description(mode) << ")";
  } else if (is_count_star(*this)) {
    if (mode == DescriptionMode::ColumnName) {
      stream << "COUNT(*)";
    } else {
      const auto* const column_expression = dynamic_cast<const LQPColumnExpression*>(&*operand());
      DebugAssert(column_expression, "Expected aggregate argument to be LQPColumnExpression");
      stream << "COUNT(" << column_expression->original_node.lock() << ".*)";
    }
  } else {
    stream << aggregate_function << "(";
    if (operand()) {
      stream << operand()->description(mode);
    }
    stream << ")";
  }

  const auto& window = this->window();
  if (window) {
    stream << " OVER (" << window->description() << ")";
  }

  return stream.str();
}

DataType WindowFunctionExpression::data_type() const {
  if (aggregate_function == WindowFunction::Count) {
    return AggregateTraits<NullValue, WindowFunction::Count>::AGGREGATE_DATA_TYPE;
  }

  Assert(_has_operand, "Expected this WindowFunction to have an operand.");

  if (aggregate_function == WindowFunction::CountDistinct) {
    return AggregateTraits<NullValue, WindowFunction::CountDistinct>::AGGREGATE_DATA_TYPE;
  }

  const auto argument_data_type = operand()->data_type();
  auto aggregate_data_type = DataType::Null;

  resolve_data_type(argument_data_type, [&](const auto data_type_t) {
    using AggregateDataType = typename decltype(data_type_t)::type;
    switch (aggregate_function) {
      case WindowFunction::Min:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Min>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::Max:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Max>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::Avg:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Avg>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::Count:
      case WindowFunction::CountDistinct:
        break;  // These are handled above
      case WindowFunction::Sum:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Sum>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::StandardDeviationSample:
        aggregate_data_type =
            AggregateTraits<AggregateDataType, WindowFunction::StandardDeviationSample>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::Any:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Any>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::CumeDist:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::CumeDist>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::DenseRank:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::DenseRank>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::PercentRank:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::PercentRank>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::Rank:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::Rank>::AGGREGATE_DATA_TYPE;
        break;
      case WindowFunction::RowNumber:
        aggregate_data_type = AggregateTraits<AggregateDataType, WindowFunction::RowNumber>::AGGREGATE_DATA_TYPE;
        break;
    }
  });

  return aggregate_data_type;
}

bool WindowFunctionExpression::is_count_star(const AbstractExpression& expression) {
  // COUNT(*) is represented by an WindowFunctionExpression with the COUNT function and an INVALID_COLUMN_ID.
  if (expression.type != ExpressionType::WindowFunction) {
    return false;
  }

  const auto& aggregate_expression = static_cast<const WindowFunctionExpression&>(expression);
  if (aggregate_expression.aggregate_function != WindowFunction::Count ||
      aggregate_expression.operand()->type != ExpressionType::LQPColumn) {
    return false;
  }

  const auto& lqp_column_expression = static_cast<LQPColumnExpression&>(*aggregate_expression.operand());
  return lqp_column_expression.original_column_id == INVALID_COLUMN_ID;
}

bool WindowFunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const WindowFunctionExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return aggregate_function == static_cast<const WindowFunctionExpression&>(expression).aggregate_function;
}

size_t WindowFunctionExpression::_shallow_hash() const {
  return boost::hash_value(static_cast<size_t>(aggregate_function));
}

bool WindowFunctionExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  // Aggregates (except COUNT and COUNT DISTINCT) will return NULL when executed on an
  // empty group - thus they are always nullable
  return aggregate_function != WindowFunction::Count && aggregate_function != WindowFunction::CountDistinct;
}

std::ostream& operator<<(std::ostream& stream, const WindowFunction window_function) {
  return stream << window_function_to_string.left.at(window_function);
}

}  // namespace hyrise
