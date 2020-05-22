#include "typed_placeholder_expression.hpp"

namespace opossum {

TypedPlaceholderExpression::TypedPlaceholderExpression(const ParameterID init_parameter_id,
                                                       const DataType init_data_type)
    : PlaceholderExpression(init_parameter_id), _data_type{init_data_type} {
  /* This type of placeholder is used for cache parameterization, where null values should not be replaced by
   * placeholers. */
  assert(_data_type != DataType::Null);
}

DataType TypedPlaceholderExpression::data_type() const { return _data_type; }

std::string TypedPlaceholderExpression::description(const DescriptionMode mode) const {
  std::stringstream stream;
  stream << "Typed Placeholder[id=" << std::to_string(parameter_id) << "]";
  return stream.str();
}

std::shared_ptr<AbstractExpression> TypedPlaceholderExpression::deep_copy() const {
  return std::make_shared<TypedPlaceholderExpression>(parameter_id, _data_type);
}

bool TypedPlaceholderExpression::_on_is_nullable_on_lqp(const AbstractLQPNode& lqp) const { return false; }

}  // namespace opossum
