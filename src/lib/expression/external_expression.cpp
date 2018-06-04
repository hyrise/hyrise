#include "external_expression.hpp"

#include "boost/functional/hash.hpp"

#include <sstream>

namespace opossum {

ExternalExpression::ExternalExpression(const ValuePlaceholder& value_placeholder,
                                       const DataType data_type,
                                       const bool nullable,
                                       const std::string& column_name):
AbstractExpression(ExpressionType::External, {}), value_placeholder(value_placeholder), _data_type(data_type), _nullable(nullable), _column_name(column_name) {

}

std::shared_ptr<AbstractExpression> ExternalExpression::deep_copy() const  {
  return std::make_shared<ExternalExpression>(value_placeholder, _data_type, _nullable, _column_name);
}

std::string ExternalExpression::as_column_name() const {
  return _column_name;
}

DataType ExternalExpression::data_type() const {
  return _data_type;
}

bool ExternalExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& external_expression = static_cast<const ExternalExpression&>(expression);
  return value_placeholder == external_expression.value_placeholder &&
         _data_type == external_expression._data_type &&
        _nullable == external_expression._nullable &&
        _column_name == external_expression._column_name;
}

size_t ExternalExpression::_on_hash() const {
  auto hash = boost::hash_value(value_placeholder.index());
  boost::hash_combine(hash, static_cast<size_t>(_data_type));
  boost::hash_combine(hash, _nullable);
  boost::hash_combine(hash, _column_name);
  return hash;
}

}