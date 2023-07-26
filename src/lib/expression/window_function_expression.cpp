#include "window_function_expression.hpp"

#include <sstream>

#include <boost/container_hash/hash.hpp>

#include "expression_utils.hpp"
#include "lqp_column_expression.hpp"
#include "operators/aggregate/window_function_traits.hpp"
#include "resolve_type.hpp"
#include "utils/assert.hpp"

namespace hyrise {

WindowFunctionExpression::WindowFunctionExpression(const WindowFunction init_window_function,
                                                   const std::shared_ptr<AbstractExpression>& argument,
                                                   const std::shared_ptr<AbstractExpression>& window)
    : AbstractExpression{ExpressionType::WindowFunction, {/* Expressions are set below. */}},
      window_function(init_window_function) {
  arguments.reserve(argument && window ? 2 : 1);
  if (argument) {
    arguments.emplace_back(argument);
  }
  if (window) {
    arguments.emplace_back(window);
    Assert(window->type == ExpressionType::Window, "Window description must be a WindowExpression");
    _has_window = true;
  }

  Assert(!arguments.empty(), "WindowFunctionExpression must not be empty.");
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::argument() const {
  if (!_has_window || arguments.size() > 1) {
    return arguments[0];
  }
  return nullptr;
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::window() const {
  return _has_window ? arguments.back() : nullptr;
}

std::string WindowFunctionExpression::description(const DescriptionMode mode) const {
  auto stream = std::stringstream{};

  if (window_function == WindowFunction::CountDistinct) {
    Assert(argument(), "COUNT(DISTINCT ...) requires an argument");
    stream << "COUNT(DISTINCT " << argument()->description(mode) << ")";
  } else if (is_count_star(*this)) {
    if (mode == DescriptionMode::ColumnName) {
      stream << "COUNT(*)";
    } else {
      const auto* const column_expression = dynamic_cast<const LQPColumnExpression*>(&*argument());
      DebugAssert(column_expression, "Expected aggregate argument to be LQPColumnExpression");
      stream << "COUNT(" << column_expression->original_node.lock() << ".*)";
    }
  } else {
    stream << window_function << "(";
    if (argument()) {
      stream << argument()->description(mode);
    }
    stream << ")";
  }

  const auto& window = this->window();
  if (window && mode == DescriptionMode::Detailed) {
    stream << " OVER (" << window->description() << ")";
  }

  return stream.str();
}

DataType WindowFunctionExpression::data_type() const {
  if (window_function == WindowFunction::Count || window_function == WindowFunction::CountDistinct ||
      !aggregate_functions.contains(window_function)) {
    switch (window_function) {
      case WindowFunction::Count:
        return WindowFunctionTraits<NullValue, WindowFunction::Count>::RESULT_TYPE;
      case WindowFunction::CountDistinct:
        return WindowFunctionTraits<NullValue, WindowFunction::CountDistinct>::RESULT_TYPE;
      case WindowFunction::CumeDist:
        return WindowFunctionTraits<NullValue, WindowFunction::CumeDist>::RESULT_TYPE;
      case WindowFunction::DenseRank:
        return WindowFunctionTraits<NullValue, WindowFunction::DenseRank>::RESULT_TYPE;
      case WindowFunction::PercentRank:
        return WindowFunctionTraits<NullValue, WindowFunction::PercentRank>::RESULT_TYPE;
      case WindowFunction::Rank:
        return WindowFunctionTraits<NullValue, WindowFunction::Rank>::RESULT_TYPE;
      case WindowFunction::RowNumber:
        return WindowFunctionTraits<NullValue, WindowFunction::RowNumber>::RESULT_TYPE;
      default:
        break;  // The rest is handled below.
    }
  }

  const auto& argument = this->argument();
  Assert(argument, "Expected " + window_function_to_string.left.at(window_function) + " to have an argument.");
  const auto argument_data_type = argument->data_type();
  auto result_type = DataType::Null;

  resolve_data_type(argument_data_type, [&](const auto data_type_t) {
    using AggregateDataType = typename decltype(data_type_t)::type;
    switch (window_function) {
      case WindowFunction::Min:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::Min>::RESULT_TYPE;
        break;
      case WindowFunction::Max:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::Max>::RESULT_TYPE;
        break;
      case WindowFunction::Avg:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::Avg>::RESULT_TYPE;
        break;
      case WindowFunction::Sum:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::Sum>::RESULT_TYPE;
        break;
      case WindowFunction::StandardDeviationSample:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::StandardDeviationSample>::RESULT_TYPE;
        break;
      case WindowFunction::Any:
        result_type = WindowFunctionTraits<AggregateDataType, WindowFunction::Any>::RESULT_TYPE;
        break;
      case WindowFunction::Count:
      case WindowFunction::CountDistinct:
      case WindowFunction::CumeDist:
      case WindowFunction::DenseRank:
      case WindowFunction::PercentRank:
      case WindowFunction::Rank:
      case WindowFunction::RowNumber:
        break;  // These are handled above.
    }
  });

  return result_type;
}

bool WindowFunctionExpression::is_count_star(const AbstractExpression& expression) {
  // COUNT(*) is represented by an WindowFunctionExpression with the COUNT function and an INVALID_COLUMN_ID.
  if (expression.type != ExpressionType::WindowFunction) {
    return false;
  }

  const auto& aggregate_expression = static_cast<const WindowFunctionExpression&>(expression);
  if (aggregate_expression.window_function != WindowFunction::Count ||
      aggregate_expression.argument()->type != ExpressionType::LQPColumn) {
    return false;
  }

  const auto& lqp_column_expression = static_cast<LQPColumnExpression&>(*aggregate_expression.argument());
  return lqp_column_expression.original_column_id == INVALID_COLUMN_ID;
}

std::shared_ptr<AbstractExpression> WindowFunctionExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  const auto& argument = this->argument();
  const auto argument_copy = argument ? argument->deep_copy(copied_ops) : nullptr;
  const auto window_copy = _has_window ? arguments.back()->deep_copy(copied_ops) : nullptr;
  return std::make_shared<WindowFunctionExpression>(window_function, argument_copy, window_copy);
}

bool WindowFunctionExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const WindowFunctionExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  return window_function == static_cast<const WindowFunctionExpression&>(expression).window_function;
}

size_t WindowFunctionExpression::_shallow_hash() const {
  return boost::hash_value(static_cast<size_t>(window_function));
}

bool WindowFunctionExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& /*lqp*/) const {
  // Pure window functions always return a value.
  if (!aggregate_functions.contains(window_function)) {
    return false;
  }

  // Aggregates (except COUNT and COUNT DISTINCT) will return NULL when executed on an empty group. Thus, they are
  // always nullable.
  return window_function != WindowFunction::Count && window_function != WindowFunction::CountDistinct;
}

std::ostream& operator<<(std::ostream& stream, const WindowFunction window_function) {
  return stream << window_function_to_string.left.at(window_function);
}

}  // namespace hyrise
