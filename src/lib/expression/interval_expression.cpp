#include "interval_expression.hpp"

#include <sstream>

#include <magic_enum.hpp>

//#include "constant_mappings.hpp"

namespace opossum {

IntervalExpression::IntervalExpression(const int64_t duration, const DatetimeComponent unit)
    : AbstractExpression(ExpressionType::Interval, {}), _duration(duration), _unit(unit) {}

DataType IntervalExpression::data_type() const { return DataType::String; }

std::shared_ptr<AbstractExpression> IntervalExpression::_on_deep_copy(
    std::unordered_map<const AbstractOperator*, std::shared_ptr<AbstractOperator>>& copied_ops) const {
  return std::make_shared<IntervalExpression>(_duration, _unit);
}

std::string IntervalExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "INTERVAL '" << _duration << " " << magic_enum::enum_name(_unit) << "'";
  return stream.str();
}

int64_t IntervalExpression::duration() const { return _duration; }

DatetimeComponent IntervalExpression::unit() const { return _unit; }

bool IntervalExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const IntervalExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& other_interval_expression = static_cast<const IntervalExpression&>(expression);
  return _duration == other_interval_expression._duration && _unit == other_interval_expression._unit;
}

size_t IntervalExpression::_shallow_hash() const {
  auto hash = boost::hash_value(static_cast<size_t>(_unit));
  boost::hash_combine(hash, _duration);
  return hash;
}

}  // namespace opossum
