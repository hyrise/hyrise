#include "jit_compute.hpp"

#include "../jit_operations.hpp"
#include "constant_mappings.hpp"

namespace opossum {

JitExpression::JitExpression(const JitTupleValue& tuple_value)
    : _expression_type{ExpressionType::Column}, _result_value{tuple_value} {}

JitExpression::JitExpression(const JitExpression::Ptr& child, const ExpressionType expression_type,
                             const size_t result_tuple_index)
    : _left_child{child},
      _expression_type{expression_type},
      _result_value{JitTupleValue(_compute_result_type(), result_tuple_index)} {}

JitExpression::JitExpression(const JitExpression::Ptr& left_child, const ExpressionType expression_type,
                             const JitExpression::Ptr& right_child, const size_t result_tuple_index)
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

void JitExpression::compute(JitRuntimeContext& ctx) const {
  // We are dealing with an already computed value here, so there is nothing to do.
  if (_expression_type == ExpressionType::Column) {
    return;
  }

  auto left_value = _left_child->result().materialize(ctx);
  auto result_value = _result_value.materialize(ctx);
  _left_child->compute(ctx);

  if (!_is_binary_operator()) {
    switch (_expression_type) {
      case ExpressionType::Not:
        jit_not(left_value, result_value);
        break;
      case ExpressionType::IsNull:
        jit_is_null(left_value, result_value);
        break;
      case ExpressionType::IsNotNull:
        jit_is_not_null(left_value, result_value);
        break;

      default:
        Fail("expression type is not supported yet");
    }
    return;
  }

  auto right_value = _right_child->result().materialize(ctx);
  _right_child->compute(ctx);

  switch (_expression_type) {
    case ExpressionType::Addition:
      jit_compute(jit_addition, left_value, right_value, result_value);
      break;
    case ExpressionType::Subtraction:
      jit_compute(jit_subtraction, left_value, right_value, result_value);
      break;
    case ExpressionType::Multiplication:
      jit_compute(jit_multiplication, left_value, right_value, result_value);
      break;
    case ExpressionType::Division:
      jit_compute(jit_division, left_value, right_value, result_value);
      break;
    case ExpressionType::Modulo:
      jit_compute(jit_modulo, left_value, right_value, result_value);
      break;
    case ExpressionType::Power:
      jit_compute(jit_power, left_value, right_value, result_value);
      break;

    case ExpressionType::Equals:
      jit_compute(jit_equals, left_value, right_value, result_value);
      break;
    case ExpressionType::NotEquals:
      jit_compute(jit_not_equals, left_value, right_value, result_value);
      break;
    case ExpressionType::GreaterThan:
      jit_compute(jit_greater_than, left_value, right_value, result_value);
      break;
    case ExpressionType::GreaterThanEquals:
      jit_compute(jit_greater_than_equals, left_value, right_value, result_value);
      break;
    case ExpressionType::LessThan:
      jit_compute(jit_less_than, left_value, right_value, result_value);
      break;
    case ExpressionType::LessThanEquals:
      jit_compute(jit_less_than_equals, left_value, right_value, result_value);
      break;
    case ExpressionType::Like:
      jit_compute(jit_like, left_value, right_value, result_value);
      break;
    case ExpressionType::NotLike:
      jit_compute(jit_not_like, left_value, right_value, result_value);
      break;

    case ExpressionType::And:
      jit_and(left_value, right_value, result_value);
      break;
    case ExpressionType::Or:
      jit_or(left_value, right_value, result_value);
      break;

    default:
      Fail("expression type not supported");
  }
}

std::pair<const JitDataType, const bool> JitExpression::_compute_result_type() {
  const auto left_result = _left_child->result();

  if (!_is_binary_operator()) {
    switch (_expression_type) {
      case ExpressionType::Not:
        return std::make_pair(JitDataType::Bool, left_result.is_nullable());
      case ExpressionType::IsNull:
      case ExpressionType::IsNotNull:
        return std::make_pair(JitDataType::Bool, false);
      default:
        Fail("expression type not supported");
    }
  }

  const auto right_result = _right_child->result();
  JitDataType result_data_type;

  switch (_expression_type) {
    case ExpressionType::Addition:
      result_data_type = jit_compute_type(jit_addition, left_result.data_type(), right_result.data_type());
      break;
    case ExpressionType::Subtraction:
      result_data_type = jit_compute_type(jit_subtraction, left_result.data_type(), right_result.data_type());
      break;
    case ExpressionType::Multiplication:
      result_data_type = jit_compute_type(jit_multiplication, left_result.data_type(), right_result.data_type());
      break;
    case ExpressionType::Division:
      result_data_type = jit_compute_type(jit_division, left_result.data_type(), right_result.data_type());
      break;
    case ExpressionType::Modulo:
      result_data_type = jit_compute_type(jit_modulo, left_result.data_type(), right_result.data_type());
      break;
    case ExpressionType::Power:
      result_data_type = jit_compute_type(jit_power, left_result.data_type(), right_result.data_type());
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
      result_data_type = JitDataType::Bool;
      break;
    default:
      Fail("expression type not supported");
  }

  return std::make_pair(result_data_type, left_result.is_nullable() || right_result.is_nullable());
}

bool JitExpression::_is_binary_operator() const {
  switch (_expression_type) {
    case ExpressionType::Addition:
    case ExpressionType::Subtraction:
    case ExpressionType::Multiplication:
    case ExpressionType::Division:
    case ExpressionType::Modulo:
    case ExpressionType::Power:
    case ExpressionType::Equals:
    case ExpressionType::NotEquals:
    case ExpressionType::GreaterThan:
    case ExpressionType::GreaterThanEquals:
    case ExpressionType::LessThan:
    case ExpressionType::LessThanEquals:
    case ExpressionType::And:
    case ExpressionType::Or:
    case ExpressionType::Like:
    case ExpressionType::NotLike:
      return true;
    default:
      return false;
  }
}

JitCompute::JitCompute(const JitExpression::Ptr& expression) : _expression{expression} {}

std::string JitCompute::description() const {
  return "[Compute] x" + std::to_string(_expression->result().tuple_index()) + " = " + _expression->to_string();
}

void JitCompute::next(JitRuntimeContext& ctx) const {
  _expression->compute(ctx);
  emit(ctx);
}

}  // namespace opossum
