#include "extract_expression.hpp"

#include <sstream>

namespace hyrise {

std::ostream& operator<<(std::ostream& stream, const DatetimeComponent datetime_component) {
  switch (datetime_component) {
    case DatetimeComponent::Year:
      stream << "YEAR";
      break;
    case DatetimeComponent::Month:
      stream << "MONTH";
      break;
    case DatetimeComponent::Day:
      stream << "DAY";
      break;
    case DatetimeComponent::Hour:
      stream << "HOUR";
      break;
    case DatetimeComponent::Minute:
      stream << "MINUTE";
      break;
    case DatetimeComponent::Second:
      stream << "SECOND";
      break;
  }
  return stream;
}

ExtractExpression::ExtractExpression(const DatetimeComponent init_datetime_component,
                                     const std::shared_ptr<AbstractExpression>& from)
    : AbstractExpression(ExpressionType::Extract, {from}), datetime_component(init_datetime_component) {}

std::shared_ptr<AbstractExpression> ExtractExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<ExtractExpression>(datetime_component, from()->deep_copy(copied_ops));
}

std::string ExtractExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "EXTRACT(" << datetime_component << " FROM " << from()->description(mode) << ")";
  return stream.str();
}

DataType ExtractExpression::data_type() const {
  // Timestamps can contain fractional seconds, so the result must be a floating-point number for seconds.
  return datetime_component == DatetimeComponent::Second ? DataType::Double : DataType::Int;
}

std::shared_ptr<AbstractExpression> ExtractExpression::from() const {
  return arguments[0];
}

bool ExtractExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const ExtractExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");

  const auto& other_extract_expression = static_cast<const ExtractExpression&>(expression);
  return other_extract_expression.datetime_component == datetime_component;
}

size_t ExtractExpression::_shallow_hash() const {
  // Hashing an enum class is a pain
  using DatetimeUnderlyingType = std::underlying_type_t<DatetimeComponent>;
  return std::hash<DatetimeUnderlyingType>{}(static_cast<DatetimeUnderlyingType>(datetime_component));
}

}  // namespace hyrise
