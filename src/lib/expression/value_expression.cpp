#include "value_expression.hpp"

#include <sstream>

#include "boost/functional/hash.hpp"
#include "resolve_type.hpp"

namespace opossum {

ValueExpression::ValueExpression(const AllTypeVariant& value): AbstractExpression(ExpressionType::Value, {}), value(value) {

}

std::shared_ptr<AbstractExpression> ValueExpression::deep_copy() const  {
  return std::make_shared<ValueExpression>(value);
}

std::string ValueExpression::as_column_name() const {
  std::stringstream stream;
  stream << value;
  return stream.str();
}

DataType ValueExpression::data_type() const {
  return data_type_from_all_type_variant(value);
}

bool ValueExpression::is_nullable() const {
  return value.type() == typeid(NullValue);
}

bool ValueExpression::_shallow_equals(const AbstractExpression& expression) const {
  return value == static_cast<const ValueExpression&>(expression).value;
}

size_t ValueExpression::_on_hash() const {
  return std::hash<AllTypeVariant>{}(value);
}

}