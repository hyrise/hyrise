#include "correlated_parameter_expression.hpp"

#include <sstream>
#include <string>
#include <type_traits>

#include "boost/functional/hash.hpp"

#include "resolve_type.hpp"

namespace opossum {

CorrelatedParameterExpression::CorrelatedParameterExpression(const ParameterID parameter_id,
                                                             const AbstractExpression& referenced_expression)
    : AbstractExpression(ExpressionType::CorrelatedParameter, {}),
      parameter_id(parameter_id),
      _referenced_expression_info(referenced_expression.data_type(), referenced_expression.as_column_name()) {}

CorrelatedParameterExpression::CorrelatedParameterExpression(const ParameterID parameter_id,
                                                             const ReferencedExpressionInfo& referenced_expression_info)
    : AbstractExpression(ExpressionType::CorrelatedParameter, {}),
      parameter_id(parameter_id),
      _referenced_expression_info(referenced_expression_info) {}

std::shared_ptr<AbstractExpression> CorrelatedParameterExpression::deep_copy() const {
  auto copy = std::make_shared<CorrelatedParameterExpression>(parameter_id, _referenced_expression_info);
  copy->_value = _value;
  return copy;
}

std::string CorrelatedParameterExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "Parameter[";
  stream << "name=" << _referenced_expression_info.column_name << ";";
  stream << "id=" << std::to_string(parameter_id);
  stream << "]";

  return stream.str();
}

bool CorrelatedParameterExpression::requires_computation() const { return false; }

DataType CorrelatedParameterExpression::data_type() const { return _referenced_expression_info.data_type; }

const std::optional<AllTypeVariant>& CorrelatedParameterExpression::value() const { return _value; }

void CorrelatedParameterExpression::set_value(const std::optional<AllTypeVariant>& value) {
  Assert(!value || data_type_from_all_type_variant(*value) == _referenced_expression_info.data_type ||
             variant_is_null(*value),
         "Invalid value assigned to CorrelatedParameterExpression");
  _value = value;
}

bool CorrelatedParameterExpression::_shallow_equals(const AbstractExpression& expression) const {
  DebugAssert(dynamic_cast<const CorrelatedParameterExpression*>(&expression),
              "Different expression type should have been caught by AbstractExpression::operator==");
  const auto& parameter_expression_rhs = static_cast<const CorrelatedParameterExpression&>(expression);

  return parameter_id == parameter_expression_rhs.parameter_id &&
         _referenced_expression_info == parameter_expression_rhs._referenced_expression_info &&
         _value == parameter_expression_rhs._value;
}

size_t CorrelatedParameterExpression::_shallow_hash() const {
  auto hash = boost::hash_value(static_cast<ParameterID::base_type>(parameter_id));

  boost::hash_combine(hash, static_cast<std::underlying_type_t<DataType>>(_referenced_expression_info.data_type));
  boost::hash_combine(hash, _referenced_expression_info.column_name);
  return hash;
}

bool CorrelatedParameterExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const {
  // Assume all correlated expression to be nullable - it is very taxing, code-wise, to determine whether
  // it actually is
  return true;
}

CorrelatedParameterExpression::ReferencedExpressionInfo::ReferencedExpressionInfo(const DataType data_type,
                                                                                  const std::string& column_name)
    : data_type(data_type), column_name(column_name) {}

bool CorrelatedParameterExpression::ReferencedExpressionInfo::operator==(const ReferencedExpressionInfo& rhs) const {
  return data_type == rhs.data_type && column_name == rhs.column_name;
}

}  // namespace opossum
