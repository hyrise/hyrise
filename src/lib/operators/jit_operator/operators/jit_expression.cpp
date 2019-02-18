#include "jit_expression.hpp"

#include <string>

#include "../jit_constant_mappings.hpp"
#include "../jit_operations.hpp"
#include "../jit_types.hpp"
#include "jit_read_tuples.hpp"

#include "expression/evaluation/like_matcher.hpp"

namespace opossum {

// Returns the enum value (e.g., DataType::Int, DataType::String) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_ENUM_VALUE(index, s) APPEND_ENUM_NAMESPACE(_, _, BOOST_PP_TUPLE_ELEM(3, 1, BOOST_PP_SEQ_ELEM(index, s)))

// Returns the data type (e.g., int32_t, std::string) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_DATA_TYPE(index, s) BOOST_PP_TUPLE_ELEM(3, 0, BOOST_PP_SEQ_ELEM(index, s))

// Call the compute function with the correct template parameter if compute is called without a template parameter and
// store the result in the runtime tuple.
#define JIT_COMPUTE_CASE(r, types)                                         \
  case JIT_GET_ENUM_VALUE(0, types): {                                     \
    const auto result = compute<JIT_GET_DATA_TYPE(0, types)>(context);     \
    _result_value.set<JIT_GET_DATA_TYPE(0, types)>(result.value, context); \
    if (_result_value.is_nullable()) {                                     \
      _result_value.set_is_null(result.is_null, context);                  \
    }                                                                      \
    break;                                                                 \
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

#define INSTANTIATE_COMPUTE_FUNCTION(r, d, type)                                                              \
  template JitValue<BOOST_PP_TUPLE_ELEM(3, 0, type)> JitExpression::compute<BOOST_PP_TUPLE_ELEM(3, 0, type)>( \
      JitRuntimeContext & context) const;

// Instantiate get and set functions for custom JitVariant
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_GET, _, JIT_DATA_TYPE_INFO)
BOOST_PP_SEQ_FOR_EACH(JIT_VARIANT_SET, _, JIT_DATA_TYPE_INFO)

JitExpression::JitExpression(const JitTupleValue& tuple_value)
    : _expression_type{JitExpressionType::Column}, _result_value{tuple_value} {}

JitExpression::JitExpression(const JitTupleValue& tuple_value, const AllTypeVariant& variant)
    : _expression_type{JitExpressionType::Value}, _result_value{tuple_value} {
  _variant.is_null = variant_is_null(variant);
  if (!_variant.is_null) {
    resolve_data_type(data_type_from_all_type_variant(variant), [&](const auto current_data_type_t) {
      using CurrentType = typename decltype(current_data_type_t)::type;
      _variant.set<CurrentType>(boost::get<CurrentType>(variant));
      // Non-jit operators store bool values as int values
      if constexpr (std::is_same_v<CurrentType, Bool>) {
        _variant.set<bool>(boost::get<CurrentType>(variant));
      }
    });
  }
}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& child, const JitExpressionType expression_type,
                             const size_t result_tuple_index)
    : _left_child{child},
      _expression_type{expression_type},
      _result_value{JitTupleValue(_compute_result_type(), result_tuple_index)} {}

JitExpression::JitExpression(const std::shared_ptr<const JitExpression>& left_child,
                             const JitExpressionType expression_type,
                             const std::shared_ptr<const JitExpression>& right_child, const size_t result_tuple_index)
    : _left_child{left_child},
      _right_child{right_child},
      _expression_type{expression_type},
      _result_value{JitTupleValue(_compute_result_type(), result_tuple_index)} {}

std::string JitExpression::to_string() const {
  if (_expression_type == JitExpressionType::Column) {
    return "x" + std::to_string(_result_value.tuple_index());
  } else if (_expression_type == JitExpressionType::Value) {
    std::stringstream str;
    if (_result_value.data_type() != DataType::Null) {
      resolve_data_type(_result_value.data_type(), [&](const auto current_data_type_t) {
        using CurrentType = typename decltype(current_data_type_t)::type;
        str << _variant.get<CurrentType>();
      });
    } else {
      str << "null";
    }
    return str.str();
  }

  const auto left = _left_child->to_string() + " ";
  const auto right = _right_child ? _right_child->to_string() + " " : "";
  return "(" + left + jit_expression_type_to_string.left.at(_expression_type) + " " + right + ")";
}

void JitExpression::compute(JitRuntimeContext& context) const {
  // We are dealing with an already computed value here, so there is nothing to do.
  if (_expression_type == JitExpressionType::Column || _expression_type == JitExpressionType::Value) {
    return;
  }

  switch (_result_value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_COMPUTE_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      break;
  }
}

std::pair<const DataType, const bool> JitExpression::_compute_result_type() {
  if (!jit_expression_is_binary(_expression_type)) {
    switch (_expression_type) {
      case JitExpressionType::Not:
        return std::make_pair(DataType::Bool, _left_child->result().is_nullable());
      case JitExpressionType::IsNull:
      case JitExpressionType::IsNotNull:
        return std::make_pair(DataType::Bool, false);
      default:
        Fail("This non-binary expression type is not supported.");
    }
  }

  DataType result_data_type;
  switch (_expression_type) {
    case JitExpressionType::Addition:
      result_data_type =
          jit_compute_type(jit_addition, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case JitExpressionType::Subtraction:
      result_data_type =
          jit_compute_type(jit_subtraction, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case JitExpressionType::Multiplication:
      result_data_type =
          jit_compute_type(jit_multiplication, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case JitExpressionType::Division:
      result_data_type =
          jit_compute_type(jit_division, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case JitExpressionType::Modulo:
      result_data_type =
          jit_compute_type(jit_modulo, _left_child->result().data_type(), _right_child->result().data_type());
      break;
    case JitExpressionType::Power:
      result_data_type =
          jit_compute_type(jit_power, _left_child->result().data_type(), _right_child->result().data_type());
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

  return std::make_pair(result_data_type, _left_child->result().is_nullable() || _right_child->result().is_nullable());
}

template <typename ResultValueType>
JitValue<ResultValueType> JitExpression::compute(JitRuntimeContext& context) const {
  if (_expression_type == JitExpressionType::Column) {
    if (_result_value.data_type() == DataType::Null) {
      return {true, ResultValueType{}};
    }
    return {_result_value.is_null(context), _result_value.get<ResultValueType>(context)};
  } else if (_expression_type == JitExpressionType::Value) {
    return {_variant.is_null, _variant.get<ResultValueType>()};
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

    if (_left_child->result().data_type() == DataType::String) {
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
  } else if constexpr (std::is_arithmetic_v<ResultValueType>) {
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
  } else {
    Fail("Expression type is not supported.");
  }
}

// Instantiate compute function for every jit data types
BOOST_PP_SEQ_FOR_EACH(INSTANTIATE_COMPUTE_FUNCTION, _, JIT_DATA_TYPE_INFO)

// cleanup
#undef JIT_GET_ENUM_VALUE
#undef JIT_GET_DATA_TYPE
#undef JIT_COMPUTE_CASE
#undef JIT_VARIANT_GET
#undef JIT_VARIANT_SET
#undef INSTANTIATE_COMPUTE_FUNCTION

}  // namespace opossum
