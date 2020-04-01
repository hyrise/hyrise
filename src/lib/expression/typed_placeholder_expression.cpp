#include "typed_placeholder_expression.hpp"

namespace opossum {

TypedPlaceholderExpression::TypedPlaceholderExpression(const ParameterID init_parameter_id, DataType init_data_type) :
  PlaceholderExpression(init_parameter_id),
  _data_type{init_data_type} {}

std::shared_ptr<AbstractExpression> TypedPlaceholderExpression::deep_copy() const {
  return std::make_shared<TypedPlaceholderExpression>(parameter_id, _data_type);
}

DataType TypedPlaceholderExpression::data_type() const { return _data_type; }

}
