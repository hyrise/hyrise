#include "jit_expression.hpp"

#include <string>

#include "../jit_constant_mappings.hpp"
#include "../jit_operations.hpp"
#include "../jit_types.hpp"

namespace opossum {

// Call the compute function with the correct template parameter if compute is called without a template parameter and
// store the result in the runtime tuple.
#define JIT_EXPRESSION_COMPUTE_CASE(r, types)                                  \
  case JIT_GET_ENUM_VALUE(0, types): {                                         \
    const auto result = compute<JIT_GET_DATA_TYPE(0, types)>(context);         \
    if (!_result_entry.is_nullable() || result.has_value()) {                  \
      _result_entry.set<JIT_GET_DATA_TYPE(0, types)>(result.value(), context); \
    }                                                                          \
    if (_result_entry.is_nullable()) {                                         \
      _result_entry.set_is_null(!result.has_value(), context);                 \
    }                                                                          \
    break;                                                                     \
  }

#define JIT_VARIANT_GET(r, d, type)                          \
  template <>                                                \
  BOOST_PP_TUPLE_ELEM(3, 0, type)                            \
  JitVariant::get<BOOST_PP_TUPLE_ELEM(3, 0, type)>() const { \
    return BOOST_PP_TUPLE_ELEM(3, 1, type);                  \
  }

#define JIT_VARIANT_SET(r, d, type)                                                                      \
  template <>                                                                                            \
  void JitVariant::set<BOOST_PP_TUPLE_ELEM(3, 0, type)>(const BOOST_PP_TUPLE_ELEM(3, 0, type) & value) { \
    BOOST_PP_TUPLE_ELEM(3, 1, type) = value;                                                             \
  }

#define INSTANTIATE_COMPUTE_FUNCTION(r, d, type)                                                                   \
  template std::optional<BOOST_PP_TUPLE_ELEM(3, 0, type)> JitExpression::compute<BOOST_PP_TUPLE_ELEM(3, 0, type)>( \
      JitRuntimeContext & context) const;

// Instantiate get and set functions for custom JitVariant
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_SET, _, JIT_DATA_TYPE_INFO)

JitVariant::JitVariant(const AllTypeVariant& variant) : is_null{variant_is_null(variant)} {
  boost::apply_visitor(
      [&](auto converted_value) {
        using CurrentType = decltype(converted_value);
        if constexpr (std::is_same_v<CurrentType, NullValue>) return;

        set<CurrentType>(converted_value);
        // Non-jit operators store bool values as int values
        if constexpr (std::is_same_v<CurrentType, opossum::Bool>) {  // opossum::Bool != JitVariant::Bool
          set<bool>(boost::get<CurrentType>(variant));
        }
      },
      variant);
}

JitExpression::JitExpression(const JitTupleEntry& tuple_entry)
    : _expression_type{JitExpressionType::Column}, _result_entry{tuple_entry} {}

JitExpression::JitExpression(const JitTupleEntry& tuple_entry, const AllTypeVariant& variant)
    : _expression_type{JitExpressionType::Value}, _result_entry{tuple_entry}, _variant{variant} {}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& child, const JitExpressionType expression_type,
                             const size_t result_tuple_index)
    : _left_child{child},
      _expression_type{expression_type},
      _result_entry{JitTupleEntry(_compute_result_type(), result_tuple_index)} {}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& left_child,
                             const JitExpressionType expression_type,
                             const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index)
    : _left_child{left_child},
      _right_child{right_child},
      _expression_type{expression_type},
      _result_entry{JitTupleEntry(_compute_result_type(), result_tuple_index)} {}

std::string JitExpression::to_string() const {
  if (_expression_type == JitExpressionType::Column) {
    return "x" + std::to_string(_result_entry.tuple_index());
  } else if (_expression_type == JitExpressionType::Value) {
    if (_result_entry.data_type() != DataType::Null) {
      // JitVariant does not have a operator<<() function.
      std::stringstream str;
      resolve_data_type(_result_entry.data_type(), [&](const auto current_data_type_t) {
        using CurrentType = typename decltype(current_data_type_t)::type;
        str << _variant.get<CurrentType>();
      });
      return str.str();
    } else {
      return "NULL";
    }
  }

  const auto left = _left_child->to_string() + " ";
  const auto right = _right_child ? _right_child->to_string() + " " : "";
  return "(" + left + jit_expression_type_to_string.left.at(_expression_type) + " " + right + ")";
}

void JitExpression::compute_and_store(JitRuntimeContext& context) const {
  // We are dealing with an already computed value here, so there is nothing to do.
  if (_expression_type == JitExpressionType::Column || _expression_type == JitExpressionType::Value) {
    return;
  }

  // Compute result value using compute<ResultValueType>() function and store it in the runtime tuple
  switch (_result_entry.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_EXPRESSION_COMPUTE_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      break;
  }
}

std::pair<const DataType, const bool> JitExpression::_compute_result_type() {
  const auto& left_tuple_entry = _left_child->result_entry();

  if (!jit_expression_is_binary(_expression_type)) {
    switch (_expression_type) {
      case JitExpressionType::Not:
        return std::make_pair(DataType::Bool, left_tuple_entry.is_nullable());
      case JitExpressionType::IsNull:
      case JitExpressionType::IsNotNull:
        return std::make_pair(DataType::Bool, false);
      default:
        Fail("This non-binary expression type is not supported.");
    }
  }

  const auto& right_tuple_entry = _right_child->result_entry();

  DataType result_data_type;
  switch (_expression_type) {
    case JitExpressionType::Addition:
      result_data_type = jit_compute_type(jit_addition, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Subtraction:
      result_data_type = jit_compute_type(jit_subtraction, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Multiplication:
      result_data_type =
          jit_compute_type(jit_multiplication, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Division:
      result_data_type = jit_compute_type(jit_division, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Modulo:
      result_data_type = jit_compute_type(jit_modulo, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Power:
      result_data_type = jit_compute_type(jit_power, left_tuple_entry.data_type(), right_tuple_entry.data_type());
      break;
    case JitExpressionType::Equals:
    case JitExpressionType::NotEquals:
    case JitExpressionType::GreaterThan:
    case JitExpressionType::GreaterThanEquals:
    case JitExpressionType::LessThan:
    case JitExpressionType::LessThanEquals:
    case JitExpressionType::Like:
    case JitExpressionType::NotLike:
    case JitExpressionType::And:
    case JitExpressionType::Or:
      result_data_type = DataType::Bool;
      break;
    default:
      Fail("This binary expression type is not supported.");
  }

  return std::make_pair(result_data_type, left_tuple_entry.is_nullable() || right_tuple_entry.is_nullable());
}

template <typename ResultValueType>
std::optional<ResultValueType> JitExpression::compute(JitRuntimeContext& context) const {
  if (_expression_type == JitExpressionType::Column) {
    if (_result_entry.data_type() == DataType::Null ||
        (_result_entry.is_nullable() && _result_entry.is_null(context))) {
      return std::nullopt;
    }
    return _result_entry.get<ResultValueType>(context);

  } else if (_expression_type == JitExpressionType::Value) {
    if (_result_entry.is_nullable() && _variant.is_null) {
      return std::nullopt;
    }
    return _variant.get<ResultValueType>();
  }

  // We check for the result type here to reduce the size of the instantiated templated functions.
  if constexpr (std::is_same_v<ResultValueType, bool>) {
    if (!jit_expression_is_binary(_expression_type)) {
      switch (_expression_type) {
        case JitExpressionType::Not:
          return jit_not(*_left_child, context);
        case JitExpressionType::IsNull:
          return jit_is_null(*_left_child, context);
        case JitExpressionType::IsNotNull:
          return jit_is_not_null(*_left_child, context);
        default:
          Fail("This non-binary expression type is not supported.");
      }
    }

    if (_left_child->result_entry().data_type() == DataType::String) {
      switch (_expression_type) {
        case JitExpressionType::Equals:
          return jit_compute<ResultValueType>(jit_string_equals, *_left_child, *_right_child, context);
        case JitExpressionType::NotEquals:
          return jit_compute<ResultValueType>(jit_string_not_equals, *_left_child, *_right_child, context);
        case JitExpressionType::GreaterThan:
          return jit_compute<ResultValueType>(jit_string_greater_than, *_left_child, *_right_child, context);
        case JitExpressionType::GreaterThanEquals:
          return jit_compute<ResultValueType>(jit_string_greater_than_equals, *_left_child, *_right_child, context);
        case JitExpressionType::LessThan:
          return jit_compute<ResultValueType>(jit_string_less_than, *_left_child, *_right_child, context);
        case JitExpressionType::LessThanEquals:
          return jit_compute<ResultValueType>(jit_string_less_than_equals, *_left_child, *_right_child, context);
        case JitExpressionType::Like:
          return jit_compute<ResultValueType>(jit_like, *_left_child, *_right_child, context);
        case JitExpressionType::NotLike:
          return jit_compute<ResultValueType>(jit_not_like, *_left_child, *_right_child, context);
        default:
          Fail("This expression type is not supported for left operand type string.");
      }
    }

    switch (_expression_type) {
      case JitExpressionType::Equals:
        return jit_compute<ResultValueType>(jit_equals, *_left_child, *_right_child, context);
      case JitExpressionType::NotEquals:
        return jit_compute<ResultValueType>(jit_not_equals, *_left_child, *_right_child, context);
      case JitExpressionType::GreaterThan:
        return jit_compute<ResultValueType>(jit_greater_than, *_left_child, *_right_child, context);
      case JitExpressionType::GreaterThanEquals:
        return jit_compute<ResultValueType>(jit_greater_than_equals, *_left_child, *_right_child, context);
      case JitExpressionType::LessThan:
        return jit_compute<ResultValueType>(jit_less_than, *_left_child, *_right_child, context);
      case JitExpressionType::LessThanEquals:
        return jit_compute<ResultValueType>(jit_less_than_equals, *_left_child, *_right_child, context);

      case JitExpressionType::And:
        return jit_and(*_left_child, *_right_child, context);
      case JitExpressionType::Or:
        return jit_or(*_left_child, *_right_child, context);
      default:
        Fail("This expression type is not supported for result type bool.");
    }
  } else if constexpr (std::is_arithmetic_v<ResultValueType>) {  // NOLINT(readability/braces)
    switch (_expression_type) {
      case JitExpressionType::Addition:
        return jit_compute<ResultValueType>(jit_addition, *_left_child, *_right_child, context);
      case JitExpressionType::Subtraction:
        return jit_compute<ResultValueType>(jit_subtraction, *_left_child, *_right_child, context);
      case JitExpressionType::Multiplication:
        return jit_compute<ResultValueType>(jit_multiplication, *_left_child, *_right_child, context);
      case JitExpressionType::Division:
        return jit_compute<ResultValueType>(jit_division, *_left_child, *_right_child, context);
      case JitExpressionType::Modulo:
        return jit_compute<ResultValueType>(jit_modulo, *_left_child, *_right_child, context);
      case JitExpressionType::Power:
        return jit_compute<ResultValueType>(jit_power, *_left_child, *_right_child, context);
      default:
        Fail("This expression type is not supported for an arithmetic result type.");
    }
  } else {  // ResultValueType == string
    Fail("Expression type is not supported.");
  }
}

// Instantiate compute function for every jit data types
BOOST_PP_SEQ_FOR_EACH(INSTANTIATE_COMPUTE_FUNCTION, _, JIT_DATA_TYPE_INFO)

// cleanup
#undef JIT_EXPRESSION_COMPUTE_CASE
#undef JIT_VARIANT_GET
#undef JIT_VARIANT_SET
#undef INSTANTIATE_COMPUTE_FUNCTION

}  // namespace opossum
