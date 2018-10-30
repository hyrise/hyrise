#include "parameter_expression.hpp"

#include <sstream>
#include <string>
#include <type_traits>

#include "boost/functional/hash.hpp"

#include "resolve_type.hpp"

namespace opossum {

ParameterExpression::ParameterExpression(const ParameterID parameter_id)
    : AbstractExpression(ExpressionType::Parameter, {}),
      parameter_id(parameter_id),
      parameter_expression_type(ParameterExpressionType::ValuePlaceholder) {}

ParameterExpression::ParameterExpression(const ParameterID parameter_id,
                                         const AbstractExpression& referenced_expression)
    : AbstractExpression(ExpressionType::Parameter, {}),
      parameter_id(parameter_id),
      parameter_expression_type(ParameterExpressionType::External),
      _referenced_expression_info(std::in_place, referenced_expression.data_type(), referenced_expression.is_nullable(),
                                  referenced_expression.as_column_name()) {}

ParameterExpression::ParameterExpression(const ParameterID parameter_id,
                                         const ReferencedExpressionInfo& referenced_expression_info)
    : AbstractExpression(ExpressionType::Parameter, {}),
      parameter_id(parameter_id),
      parameter_expression_type(ParameterExpressionType::External),
      _referenced_expression_info(referenced_expression_info) {}

bool ParameterExpression::requires_computation() const { return false; }

std::shared_ptr<AbstractExpression> ParameterExpression::deep_copy() const {
  if (_referenced_expression_info) {
    return std::make_shared<ParameterExpression>(parameter_id, *_referenced_expression_info);
  } else {
    return std::make_shared<ParameterExpression>(parameter_id);
  }
}

std::string ParameterExpression::as_column_name() const {
  std::stringstream stream;
  stream << "Parameter[";
  if (parameter_expression_type == ParameterExpressionType::External) {
    stream << "name=" << _referenced_expression_info->column_name << ";";
  }
  stream << "id=" << std::to_string(parameter_id);

  stream << "]";

  if (_value) {
    stream << "=" << *_value;
  }

  return stream.str();
}

DataType ParameterExpression::data_type() const {
  if (parameter_expression_type == ParameterExpressionType::ValuePlaceholder) {
    Assert(_value.has_value(), "Can't obtain type of unset ValuePlaceholder");
    return data_type_from_all_type_variant(*_value);
  } else {
    return _referenced_expression_info->data_type;
  }
}

bool ParameterExpression::is_nullable() const {
  if (parameter_expression_type == ParameterExpressionType::ValuePlaceholder) {
    return true;
  } else {
    return _referenced_expression_info->nullable;
  }
}

const std::optional<AllTypeVariant>& ParameterExpression::value() const { return _value; }

void ParameterExpression::set_value(const std::optional<AllTypeVariant>& value) {
  if (!value) {
    _value.reset();
  } else {
    Assert(parameter_expression_type == ParameterExpressionType::ValuePlaceholder ||
               data_type_from_all_type_variant(*value) == _referenced_expression_info->data_type,
           "Can't set Parameter to this DataType");
    _value = value;
  }
}

bool ParameterExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& parameter_expression_rhs = static_cast<const ParameterExpression&>(expression);

  // For the purpose of comparing ParameterExpressions, Null==Null
  const auto both_are_null = _value && parameter_expression_rhs._value &&
                             variant_is_null(*parameter_expression_rhs._value) && variant_is_null(*_value);

  return parameter_id == parameter_expression_rhs.parameter_id &&
         parameter_expression_type == parameter_expression_rhs.parameter_expression_type &&
         (both_are_null || _value == parameter_expression_rhs._value) &&
         _referenced_expression_info == parameter_expression_rhs._referenced_expression_info;
}

size_t ParameterExpression::_on_hash() const {
  auto hash = boost::hash_value(parameter_id.t);

  boost::hash_combine(hash, static_cast<std::underlying_type_t<ParameterExpressionType>>(parameter_expression_type));
  boost::hash_combine(hash, std::hash<AllTypeVariant>{}(_value.has_value()));
  boost::hash_combine(hash, std::hash<AllTypeVariant>{}(_value.value_or(NullValue{})));
  boost::hash_combine(hash, _referenced_expression_info.has_value());

  if (_referenced_expression_info) {
    boost::hash_combine(hash, static_cast<std::underlying_type_t<DataType>>(_referenced_expression_info->data_type));
    boost::hash_combine(hash, _referenced_expression_info->nullable);
    boost::hash_combine(hash, _referenced_expression_info->column_name);
  }

  return hash;
}

ParameterExpression::ReferencedExpressionInfo::ReferencedExpressionInfo(const DataType data_type, const bool nullable,
                                                                        const std::string& column_name)
    : data_type(data_type), nullable(nullable), column_name(column_name) {}

bool ParameterExpression::ReferencedExpressionInfo::operator==(const ReferencedExpressionInfo& rhs) const {
  return data_type == rhs.data_type && nullable == rhs.nullable && column_name == rhs.column_name;
}

}  // namespace opossum
