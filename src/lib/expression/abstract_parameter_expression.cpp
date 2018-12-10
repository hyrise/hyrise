#include "abstract_parameter_expression.hpp"

namespace opossum {

AbstractParameterExpression::AbstractParameterExpression(const ParameterExpressionType parameter_expression_type, const ParameterID parameter_id)
    : AbstractExpression(ExpressionType::Parameter, {}), parameter_expression_type(parameter_expression_type),
      parameter_id(parameter_id) {}


bool AbstractParameterExpression::requires_computation() const { return false; }

}  // namespace opossum
