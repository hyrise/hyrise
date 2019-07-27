#include "jit_expression.hpp"

#include <string>

#include "../jit_constant_mappings.hpp"
#include "../jit_operations.hpp"
#include "../jit_types.hpp"

namespace opossum {

// Call the compute function with the correct template parameter if compute is called without a template parameter and
// store the result in the runtime tuple.
#define JIT_EXPRESSION_COMPUTE_CASE(r, types)                                 \
  case JIT_GET_ENUM_VALUE(0, types): {                                        \
    const auto result = compute<JIT_GET_DATA_TYPE(0, types)>(context);        \
    if (result_entry.guaranteed_non_null || result) {                         \
      result_entry.set<JIT_GET_DATA_TYPE(0, types)>(result.value(), context); \
    }                                                                         \
    if (!result_entry.guaranteed_non_null) {                                  \
      result_entry.set_is_null(!result, context);                             \
    }                                                                         \
    break;                                                                    \
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
    : expression_type{JitExpressionType::Column}, result_entry{tuple_entry} {}

JitExpression::JitExpression(const JitTupleEntry& tuple_entry, const AllTypeVariant& variant)
    : expression_type{JitExpressionType::Value}, result_entry{tuple_entry}, _variant{variant} {}

JitExpression::JitExpression(const std::shared_ptr<JitExpression>& child, const JitExpressionType expression_type,
                             const size_t result_tuple_index)
    : left_child{child},
      expression_type{expression_type},
      result_entry{JitTupleEntry(_compute_result_type(), result_tuple_index)} {}

JitExpression::JitExpression(const std::shared_ptr<JitExpression>& left_child, const JitExpressionType expression_type,
                             const std::shared_ptr<JitExpression>& right_child, const size_t result_tuple_index)
    : left_child{left_child},
      right_child{right_child},
      expression_type{expression_type},
      result_entry{JitTupleEntry(_compute_result_type(), result_tuple_index)} {}

std::string JitExpression::to_string() const {
  if (expression_type == JitExpressionType::Column) {
    return "x" + std::to_string(result_entry.tuple_index);
  } else if (expression_type == JitExpressionType::Value) {
    if (result_entry.data_type != DataType::Null) {
      // JitVariant does not have a operator<<() function.
      std::stringstream str;
      resolve_data_type(result_entry.data_type, [&](const auto current_data_type_t) {
        using CurrentType = typename decltype(current_data_type_t)::type;
        str << _variant.get<CurrentType>();
      });
      return str.str();
    } else {
      return "NULL";
    }
  }

  const auto left = left_child->to_string() + " ";
  const auto right = right_child ? right_child->to_string() + " " : "";
  return "(" + left + jit_expression_type_to_string.left.at(expression_type) + " " + right + ")";
}

void JitExpression::compute_and_store(JitRuntimeContext& context) const {
  // We are dealing with an already computed value here, so there is nothing to do.
  if (expression_type == JitExpressionType::Column || expression_type == JitExpressionType::Value) {
    return;
  }

  // Compute result value using compute<ResultValueType>() function and store it in the runtime tuple
  switch (result_entry.data_type) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_EXPRESSION_COMPUTE_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      break;
  }
}

std::pair<const DataType, const bool> JitExpression::_compute_result_type() {
  const auto& left_tuple_entry = left_child->result_entry;

  if (!jit_expression_is_binary(expression_type)) {
    switch (expression_type) {
      case JitExpressionType::Not:
        return std::make_pair(DataType::Bool, left_tuple_entry.guaranteed_non_null);
      case JitExpressionType::IsNull:
      case JitExpressionType::IsNotNull:
        return std::make_pair(DataType::Bool, true);
      default:
        Fail("This non-binary expression type is not supported.");
    }
  }

  const auto& right_tuple_entry = right_child->result_entry;

  DataType result_data_type;
  switch (expression_type) {
    case JitExpressionType::Addition:
      result_data_type = jit_compute_type(jit_addition, left_tuple_entry.data_type, right_tuple_entry.data_type);
      break;
    case JitExpressionType::Subtraction:
      result_data_type = jit_compute_type(jit_subtraction, left_tuple_entry.data_type, right_tuple_entry.data_type);
      break;
    case JitExpressionType::Multiplication:
      result_data_type = jit_compute_type(jit_multiplication, left_tuple_entry.data_type, right_tuple_entry.data_type);
      break;
    case JitExpressionType::Division:
      result_data_type = jit_compute_type(jit_division, left_tuple_entry.data_type, right_tuple_entry.data_type);
      break;
    case JitExpressionType::Modulo:
      result_data_type = jit_compute_type(jit_modulo, left_tuple_entry.data_type, right_tuple_entry.data_type);
      break;
    case JitExpressionType::Power:
      result_data_type = jit_compute_type(jit_power, left_tuple_entry.data_type, right_tuple_entry.data_type);
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

  const bool guaranteed_non_null = left_tuple_entry.guaranteed_non_null && right_tuple_entry.guaranteed_non_null &&
                                   expression_type != JitExpressionType::Division &&
                                   expression_type != JitExpressionType::Modulo;
  return std::make_pair(result_data_type, guaranteed_non_null);
}

template <typename ResultValueType>
std::optional<ResultValueType> JitExpression::compute(JitRuntimeContext& context) const {
  // Value ids are always retrieved from the runtime tuple
  // NOLINTNEXTLINE(bugprone-suspicious-semicolon,misc-suspicious-semicolon) clang tidy identifies a false positive (https://reviews.llvm.org/D46027)
  if constexpr (std::is_same_v<ResultValueType, ValueID>) {
    if (result_entry.data_type == DataType::Null || result_entry.is_null(context)) {
      return std::nullopt;
    }
    return result_entry.get<ValueID>(context);
  }

  if (expression_type == JitExpressionType::Column) {
    if (result_entry.data_type == DataType::Null || result_entry.is_null(context)) {
      return std::nullopt;
    }
    return result_entry.get<ResultValueType>(context);

  } else if (expression_type == JitExpressionType::Value) {
    if (!result_entry.guaranteed_non_null && _variant.is_null) {
      return std::nullopt;
    }
    return _variant.get<ResultValueType>();
  }

  // We check for the result type here to reduce the size of the instantiated templated functions.
  if constexpr (std::is_same_v<ResultValueType, bool>) {
    if (!jit_expression_is_binary(expression_type)) {
      switch (expression_type) {
        case JitExpressionType::Not:
          return jit_not(*left_child, context);
        case JitExpressionType::IsNull:
          return jit_is_null(*left_child, context, use_value_ids);
        case JitExpressionType::IsNotNull:
          return jit_is_not_null(*left_child, context, use_value_ids);
        default:
          Fail("This non-binary expression type is not supported.");
      }
    }

    if (left_child->result_entry.data_type == DataType::String && !use_value_ids) {
      switch (expression_type) {
        case JitExpressionType::Equals:
          return jit_compute<ResultValueType>(jit_string_equals, *left_child, *right_child, context);
        case JitExpressionType::NotEquals:
          return jit_compute<ResultValueType>(jit_string_not_equals, *left_child, *right_child, context);
        case JitExpressionType::GreaterThan:
          return jit_compute<ResultValueType>(jit_string_greater_than, *left_child, *right_child, context);
        case JitExpressionType::GreaterThanEquals:
          return jit_compute<ResultValueType>(jit_string_greater_than_equals, *left_child, *right_child, context);
        case JitExpressionType::LessThan:
          return jit_compute<ResultValueType>(jit_string_less_than, *left_child, *right_child, context);
        case JitExpressionType::LessThanEquals:
          return jit_compute<ResultValueType>(jit_string_less_than_equals, *left_child, *right_child, context);
        case JitExpressionType::Like:
          return jit_compute<ResultValueType>(jit_like, *left_child, *right_child, context);
        case JitExpressionType::NotLike:
          return jit_compute<ResultValueType>(jit_not_like, *left_child, *right_child, context);
        default:
          Fail("This expression type is not supported for left operand type string.");
      }
    }

    switch (expression_type) {
      case JitExpressionType::Equals:
        return jit_compute<ResultValueType>(jit_equals, *left_child, *right_child, context, use_value_ids);
      case JitExpressionType::NotEquals:
        return jit_compute<ResultValueType>(jit_not_equals, *left_child, *right_child, context, use_value_ids);
      case JitExpressionType::GreaterThan:
        if (!use_value_ids) {
          return jit_compute<ResultValueType>(jit_greater_than, *left_child, *right_child, context);
        }
        [[fallthrough]];  // use >= instead of > for value id comparisons
      case JitExpressionType::GreaterThanEquals:
        return jit_compute<ResultValueType>(jit_greater_than_equals, *left_child, *right_child, context, use_value_ids);
      case JitExpressionType::LessThanEquals:
        if (!use_value_ids) {
          return jit_compute<ResultValueType>(jit_less_than_equals, *left_child, *right_child, context);
        }
        [[fallthrough]];  // use < instead of <= for value id comparisons
      case JitExpressionType::LessThan:
        return jit_compute<ResultValueType>(jit_less_than, *left_child, *right_child, context, use_value_ids);

      case JitExpressionType::And:
        return jit_and(*left_child, *right_child, context);
      case JitExpressionType::Or:
        return jit_or(*left_child, *right_child, context);
      default:
        Fail("This expression type is not supported for result type bool.");
    }
  } else if constexpr (std::is_arithmetic_v<ResultValueType>) {  // NOLINT(readability/braces)
    switch (expression_type) {
      case JitExpressionType::Addition:
        return jit_compute<ResultValueType>(jit_addition, *left_child, *right_child, context);
      case JitExpressionType::Subtraction:
        return jit_compute<ResultValueType>(jit_subtraction, *left_child, *right_child, context);
      case JitExpressionType::Multiplication:
        return jit_compute<ResultValueType>(jit_multiplication, *left_child, *right_child, context);
      case JitExpressionType::Division:
        return jit_compute<ResultValueType, true>(jit_division, *left_child, *right_child, context);
      case JitExpressionType::Modulo:
        return jit_compute<ResultValueType, true>(jit_modulo, *left_child, *right_child, context);
      case JitExpressionType::Power:
        return jit_compute<ResultValueType>(jit_power, *left_child, *right_child, context);
      default:
        Fail("This expression type is not supported for an arithmetic result type.");
    }
  } else {  // ResultValueType == string
    Fail("Expression type is not supported.");
  }
}

void JitExpression::update_nullable_information(std::vector<bool>& tuple_non_nullable_information) {
  if (expression_type == JitExpressionType::Column) {
    result_entry.guaranteed_non_null = tuple_non_nullable_information[result_entry.tuple_index];
    return;
  }

  if (expression_type != JitExpressionType::Value) {
    left_child->update_nullable_information(tuple_non_nullable_information);
    if (jit_expression_is_binary(expression_type)) {
      right_child->update_nullable_information(tuple_non_nullable_information);
    }

    result_entry.guaranteed_non_null = _compute_result_type().second;
  }

  tuple_non_nullable_information[result_entry.tuple_index] = result_entry.guaranteed_non_null;
}

// Instantiate compute function for every jit data types
BOOST_PP_SEQ_FOR_EACH(INSTANTIATE_COMPUTE_FUNCTION, _, JIT_DATA_TYPE_INFO_WITH_VALUE_ID)

// cleanup
#undef JIT_EXPRESSION_COMPUTE_CASE
#undef JIT_VARIANT_GET
#undef JIT_VARIANT_SET
#undef INSTANTIATE_COMPUTE_FUNCTION

}  // namespace opossum
