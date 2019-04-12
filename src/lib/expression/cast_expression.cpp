#include "cast_expression.hpp"

#include <sstream>

#include "constant_mappings.hpp"

namespace opossum {

CastExpression::CastExpression(const std::shared_ptr<AbstractExpression>& argument, const DataType data_type)
    : AbstractExpression(ExpressionType::Cast, {argument}), _data_type(data_type) {}

std::shared_ptr<AbstractExpression> CastExpression::deep_copy() const {
  return std::make_shared<CastExpression>(argument()->deep_copy(), _data_type);
}

std::string CastExpression::as_column_name() const {
  std::stringstream stream;
  stream << "CAST(" << argument()->as_column_name() << " AS " << _data_type << ")";
  return stream.str();
}

DataType CastExpression::data_type() const { return _data_type; }

std::shared_ptr<AbstractExpression> CastExpression::argument() const { return arguments[0]; }

bool CastExpression::_shallow_equals(const AbstractExpression& expression) const {
  const auto& other_cast_expression = static_cast<const CastExpression&>(expression);
  return _data_type == other_cast_expression._data_type;
}

size_t CastExpression::_on_hash() const {
  // Hashing an enum class is a pain
  using DataTypeUnderlyingType = std::underlying_type_t<DataType>;
  return std::hash<DataTypeUnderlyingType>{}(static_cast<DataTypeUnderlyingType>(_data_type));
}

}  // namespace opossum
