#include "typed_placeholder_expression.hpp"

namespace opossum {

TypedPlaceholderExpression::TypedPlaceholderExpression(const ParameterID parameter_id, DataType data_type) :
  PlaceholderExpression(parameter_id),
  _data_type{data_type} {}

DataType TypedPlaceholderExpression::data_type() const { return _data_type; }

}
