#include "parameter_expression.hpp"

#include <sstream>
#include <string>
#include <type_traits>

#include "boost/functional/hash.hpp"

namespace opossum {

ParameterExpression::ParameterExpression(const ParameterID parameter_id):
  AbstractExpression(ExpressionType::Parameter, {}), parameter_id(parameter_id), parameter_expression_type(ParameterExpressionType::ValuePlaceholder)
{

}

ParameterExpression::ParameterExpression(const ParameterID parameter_id, const AbstractExpression& referenced_expression):
AbstractExpression(ExpressionType::Parameter, {}), parameter_id(parameter_id), parameter_expression_type(ParameterExpressionType::External)
{
  _referenced_expression_info.emplace(referenced_expression.data_type(), referenced_expression.is_nullable(), referenced_expression.as_column_name());
}

ParameterExpression::ParameterExpression(const ParameterID parameter_id, const ReferencedExpressionInfo& referenced_expression_info):
AbstractExpression(ExpressionType::Parameter, {}), parameter_id(parameter_id), parameter_expression_type(ParameterExpressionType::External), _referenced_expression_info(referenced_expression_info) {

}


std::shared_ptr<AbstractExpression> ParameterExpression::deep_copy() const {
  const auto copy = std::make_shared<ParameterExpression>(parameter_id);
  copy->_referenced_expression_info = _referenced_expression_info;
  return copy;
}

std::string ParameterExpression::as_column_name() const {
  std::stringstream stream;
  stream << "?" << std::to_string(parameter_id) << "?";

  if (parameter_expression_type == ParameterExpressionType::External) {
    stream << " '" <<  _referenced_expression_info->column_name << "'";
  }

  stream << "=" << value;

  return stream.str();
}

DataType ParameterExpression::data_type() const {
  Assert(parameter_expression_type != ParameterExpressionType::ValuePlaceholder, "ValuePlaceholders have no type");
  return _referenced_expression_info->data_type;
}

bool ParameterExpression::is_nullable() const {
  Assert(parameter_expression_type != ParameterExpressionType::ValuePlaceholder, "ValuePlaceholders have no type");
  return _referenced_expression_info->nullable;
}

bool ParameterExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& parameter_expression_rhs = static_cast<const ParameterExpression&>(expression);

  auto both_are_null = variant_is_null(parameter_expression_rhs.value) && variant_is_null(value);

  return parameter_id == parameter_expression_rhs.parameter_id &&
         parameter_expression_type == parameter_expression_rhs.parameter_expression_type &&
         (both_are_null || value == parameter_expression_rhs.value) &&
         _referenced_expression_info == parameter_expression_rhs._referenced_expression_info;
}

size_t ParameterExpression::_on_hash() const {
  auto hash = boost::hash_value(parameter_id.t);
  boost::hash_combine(hash, static_cast<std::underlying_type_t<ParameterExpressionType>>(parameter_expression_type));
  boost::hash_combine(hash, std::hash<AllTypeVariant>{}(value));
  boost::hash_combine(hash, _referenced_expression_info.has_value());
  if (_referenced_expression_info) {
    boost::hash_combine(hash, static_cast<std::underlying_type_t<DataType>>(_referenced_expression_info->data_type));
    boost::hash_combine(hash, _referenced_expression_info->nullable);
    boost::hash_combine(hash, _referenced_expression_info->column_name);
  } else {
    boost::hash_combine(hash, 0);
    boost::hash_combine(hash, 0);
    boost::hash_combine(hash, 0);
  }
  return hash;
}

ParameterExpression::ReferencedExpressionInfo::ReferencedExpressionInfo(const DataType data_type, const bool nullable, const std::string& column_name):
  data_type(data_type), nullable(nullable), column_name(column_name)
{

}

bool ParameterExpression::ReferencedExpressionInfo::operator==(const ReferencedExpressionInfo& rhs) const {
  return data_type == rhs.data_type && nullable == rhs.nullable && column_name == rhs.column_name;
}

}  // namespace opossum
