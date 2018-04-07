#include "jit_compute.hpp"

#include "../jit_operations.hpp"
#include "abstract_expression.hpp"
#include "constant_mappings.hpp"

namespace opossum {

JitExpression::JitExpression(const JitTupleValue& tuple_value)
    : _expression_type{ExpressionType::Column}, _result_value{tuple_value} {}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& child, const ExpressionType expression_type,
                             const size_t result_tuple_index)
    : _left_child{child},
      _expression_type{expression_type},
      _result_value{JitTupleValue(_compute_result_type(), result_tuple_index)} {}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& left_child,
                             const ExpressionType expression_type,
                             const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index)
    : _left_child{left_child},
      _right_child{right_child},
      _expression_type{expression_type},
      _result_value{JitTupleValue(_compute_result_type(), result_tuple_index)} {}

std::string JitExpression::to_string() const {
  if (_expression_type == ExpressionType::Column) {
    return "x" + std::to_string(_result_value.tuple_index());
  }

  const auto left = _left_child->to_string() + " ";
  const auto right = _right_child ? _right_child->to_string() + " " : "";
  return "(" + left + expression_type_to_string.at(_expression_type) + " " + right + ")";
}

void JitExpression::compute(JitRuntimeContext& context) const {
  // We are dealing with an already computed value here, so there is nothing to do.
  if (_expression_type == ExpressionType::Column) {
    return;
  }

  _left_child->compute(context);

  if (!is_binary_operator_type(_expression_type)) {
    switch (_expression_type) {
      case ExpressionType::Not:
        jit_not(_left_child->result(), _result_value, context);
        break;
      case ExpressionType::IsNull:
        jit_is_null(_left_child->result(), _result_value, context);
        break;
      case ExpressionType::IsNotNull:
        jit_is_not_null(_left_child->result(), _result_value, context);
        break;
      default:
        Fail("Expression type is not supported.");
        break;
    }
    return;
  }

  _right_child->compute(context);

  switch (_expression_type) {
    case ExpressionType::Addition:
      jit_compute(jit_addition, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Subtraction:
      jit_compute(jit_subtraction, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Multiplication:
      jit_compute(jit_multiplication, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Division:
      jit_compute(jit_division, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Modulo:
      jit_compute(jit_modulo, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Power:
      jit_compute(jit_power, _left_child->result(), _right_child->result(), _result_value, context);
      break;

    case ExpressionType::Equals:
      jit_compute(jit_equals, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::NotEquals:
      jit_compute(jit_not_equals, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::GreaterThan:
      jit_compute(jit_greater_than, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::GreaterThanEquals:
      jit_compute(jit_greater_than_equals, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::LessThan:
      jit_compute(jit_less_than, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::LessThanEquals:
      jit_compute(jit_less_than_equals, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Like:
      jit_compute(jit_like, _left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::NotLike:
      jit_compute(jit_not_like, _left_child->result(), _right_child->result(), _result_value, context);
      break;

    case ExpressionType::And:
      jit_and(_left_child->result(), _right_child->result(), _result_value, context);
      break;
    case ExpressionType::Or:
      jit_or(_left_child->result(), _right_child->result(), _result_value, context);
      break;
    default:
      Fail("Expression type is not supported.");
  }
}

std::pair<const DataType, const bool> JitExpression::_compute_result_type() {
  if (!is_binary_operator_type(_expression_type)) {
    switch (_expression_type) {
      case ExpressionType::Not:
        return std::make_pair(DataType::Bool, _left_child->result().is_nullable());
      case ExpressionType::IsNull:
      case ExpressionType::IsNotNull:
        return std::make_pair(DataType::Bool, false);
      default:
        Fail("Expression type not supported.");
    }
  }

  DataType result_data_type;
  switch (_expression_type) {
    case ExpressionType::Addition:
      result_data_type =
          jit_compute_type(jit_addition, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Subtraction:
      result_data_type =
          jit_compute_type(jit_subtraction, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Multiplication:
      result_data_type =
          jit_compute_type(jit_multiplication, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Division:
      result_data_type =
          jit_compute_type(jit_division, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Modulo:
      result_data_type =
          jit_compute_type(jit_modulo, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Power:
      result_data_type =
          jit_compute_type(jit_power, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
    case ExpressionType::And:
    case ExpressionType::Or:
      result_data_type = DataType::Bool;
      break;
    default:
      Fail("Expression type not supported.");
  }

  return std::make_pair(result_data_type, _left_child->result().is_nullable() || _right_child->result().is_nullable());
}

}  // namespace opossum
