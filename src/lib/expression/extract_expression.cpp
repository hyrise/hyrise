#include "extract_expression.hpp"

#include <sstream>

namespace opossum {

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

ExtractExpression::ExtractExpression(const DatetimeComponent datetime_component,
                                     const std::shared_ptr<AbstractExpression>& from)
    : AbstractExpression(ExpressionType::Extract, {from}), datetime_component(datetime_component) {}

std::shared_ptr<AbstractExpression> ExtractExpression::deep_copy() const {
  return std::make_shared<ExtractExpression>(datetime_component, from()->deep_copy());
}

std::string ExtractExpression::as_column_name() const {
  std::stringstream stream;
  stream << "EXTRACT(" << datetime_component << " FROM " << from()->as_column_name() << ")";
  return stream.str();
}

DataType ExtractExpression::data_type() const {
  // Dates are Strings, DateComponents COULD be Ints, but lets leave it at String for now
  return DataType::String;
}

std::shared_ptr<AbstractExpression> ExtractExpression::from() const { return arguments[0]; }

bool ExtractExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& other_extract_expression = static_cast<const ExtractExpression&>(expression);
  return other_extract_expression.datetime_component == datetime_component;
}

size_t ExtractExpression::_on_hash() const {
  // Hashing an enum class is a pain
  using DatetimeUnderlyingType = std::underlying_type_t<DatetimeComponent>;
  return std::hash<DatetimeUnderlyingType>{}(static_cast<DatetimeUnderlyingType>(datetime_component));
}

}  // namespace opossum
