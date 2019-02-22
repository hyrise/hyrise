#include "jit_operations.hpp"

namespace opossum {

// Returns the enum value (e.g., DataType::Int, DataType::String) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_ENUM_VALUE(index, s) APPEND_ENUM_NAMESPACE(_, _, BOOST_PP_TUPLE_ELEM(3, 1, BOOST_PP_SEQ_ELEM(index, s)))

// Returns the data type (e.g., int32_t, std::string) of a data type defined in the DATA_TYPE_INFO sequence
#define JIT_GET_DATA_TYPE(index, s) BOOST_PP_TUPLE_ELEM(3, 0, BOOST_PP_SEQ_ELEM(index, s))

#define JIT_HASH_CASE(r, types)                      \
  case JIT_GET_ENUM_VALUE(0, types):                 \
    return std::hash<JIT_GET_DATA_TYPE(0, types)>()( \
        context.tuple.get<JIT_GET_DATA_TYPE(0, types)>(value.tuple_index()));

#define JIT_AGGREGATE_EQUALS_CASE(r, types) \
  case JIT_GET_ENUM_VALUE(0, types):        \
    return lhs.get<JIT_GET_DATA_TYPE(0, types)>(context) == rhs.get<JIT_GET_DATA_TYPE(0, types)>(rhs_index, context);

#define JIT_ASSIGN_CASE(r, types)    \
  case JIT_GET_ENUM_VALUE(0, types): \
    return to.set<JIT_GET_DATA_TYPE(0, types)>(from.get<JIT_GET_DATA_TYPE(0, types)>(context), to_index, context);

#define JIT_GROW_BY_ONE_CASE(r, types) \
  case JIT_GET_ENUM_VALUE(0, types):   \
    return context.hashmap.columns[value.column_index()].grow_by_one<JIT_GET_DATA_TYPE(0, types)>(initial_value);

#define JIT_IS_NULL_CASE(r, types)                                               \
  case JIT_GET_ENUM_VALUE(0, types): {                                           \
    const auto result = left_side.compute<JIT_GET_DATA_TYPE(0, types)>(context); \
    return {false, result.is_null()};                                              \
  }

#define JIT_IS_NOT_NULL_CASE(r, types)                                           \
  case JIT_GET_ENUM_VALUE(0, types): {                                           \
    const auto result = left_side.compute<JIT_GET_DATA_TYPE(0, types)>(context); \
    return {false, !result.is_null()};                                             \
  }

JitValue<bool> jit_not(const JitExpression& left_side, JitRuntimeContext& context) {
  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert((left_side.result().data_type() == DataType::Bool || left_side.result().data_type() == DataType::Int),
              "invalid type for jit operation not");
  const auto value = left_side.compute<bool>(context);
  return {value.is_null(), !value.value()};
}

JitValue<bool> jit_and(const JitExpression& left_side, const JitExpression& right_side, JitRuntimeContext& context) {
  // three-valued logic AND

  // Get result types
  const auto lhs = left_side.result();
  const auto rhs = right_side.result();

  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert((lhs.data_type() == DataType::Bool || lhs.data_type() == DataType::Int) &&
                  (rhs.data_type() == DataType::Bool || rhs.data_type() == DataType::Int),
              "invalid type for jit operation and");

  const auto left_result = left_side.compute<bool>(context);
  // Computation of right hand side can be pruned if left result is false and not null
  if (!left_result.value()) {                            // Left result is false
    if (!lhs.is_nullable() || !left_result.is_null()) {  // Left result is not null
      return {false, false};
    }
  }

  // Left result is null or true
  const auto right_result = right_side.compute<bool>(context);
  if (lhs.is_nullable() && left_result.is_null()) {     // Left result is null
    if (rhs.is_nullable() && right_result.is_null()) {  // Right result is null
      return {true, false};
    } else {
      return {right_result.value(), false};
    }
  }

  // Left result is false and not null
  return {rhs.is_nullable() && right_result.is_null(), right_result.value()};
}

JitValue<bool> jit_or(const JitExpression& left_side, const JitExpression& right_side, JitRuntimeContext& context) {
  // three-valued logic OR

  // Get result types
  const auto lhs = left_side.result();
  const auto rhs = right_side.result();

  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert((lhs.data_type() == DataType::Bool || lhs.data_type() == DataType::Int) &&
                  (rhs.data_type() == DataType::Bool || rhs.data_type() == DataType::Int),
              "invalid type for jit operation or");

  const auto left_result = left_side.compute<bool>(context);
  // Computation of right hand side can be pruned if left result is true and not null
  if (left_result.value()) {                             // Left result is true
    if (!lhs.is_nullable() || !left_result.is_null()) {  // Left result is not null
      return {false, true};
    }
  }

  // Left result is null or false
  const auto right_result = right_side.compute<bool>(context);
  if (lhs.is_nullable() && left_result.is_null()) {     // Left result is null
    if (rhs.is_nullable() && right_result.is_null()) {  // Right result is null
      return {true, false};
    } else {
      return {!right_result.value(), true};
    }
  }

  // Left result is false and not null
  return {rhs.is_nullable() && right_result.is_null(), right_result.value()};
}

// TODO(anyone) State Machine is currently build for every comparison. It should be build only once.
bool jit_like(const std::string& a, const std::string& b) {
  const auto regex_string = LikeMatcher::sql_like_to_regex(b);
  const auto regex = std::regex{regex_string};
  return std::regex_match(a, regex);
}

// TODO(anyone) State Machine is currently build for every comparison. It should be build only once.
bool jit_not_like(const std::string& a, const std::string& b) {
  const auto regex_string = LikeMatcher::sql_like_to_regex(b);
  const auto regex = std::regex{regex_string};
  return !std::regex_match(a, regex);
}

JitValue<bool> jit_is_null(const JitExpression& left_side, JitRuntimeContext& context) {
  switch (left_side.result().data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_IS_NULL_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      return {false, true};
  }
}

JitValue<bool> jit_is_not_null(const JitExpression& left_side, JitRuntimeContext& context) {
  switch (left_side.result().data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_IS_NOT_NULL_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      return {false, false};
  }
}

uint64_t jit_hash(const JitTupleValue& value, JitRuntimeContext& context) {
  // NULL values hash to 0.
  if (value.is_null(context)) {
    return 0;
  }

  // For all other values the hash is computed by the corresponding std::hash function
  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_HASH_CASE, (JIT_DATA_TYPE_INFO))
    default:
      Fail("unreachable");
  }
}

bool jit_aggregate_equals(const JitTupleValue& lhs, const JitHashmapValue& rhs, const size_t rhs_index,
                          JitRuntimeContext& context) {
  // NULL == NULL when grouping tuples in the aggregate operator
  if (lhs.is_null(context) && rhs.is_null(rhs_index, context)) {
    return true;
  }

  if (lhs.is_null(context) || rhs.is_null(rhs_index, context)) {
    return false;
  }

  DebugAssert(lhs.data_type() == rhs.data_type(), "Data types don't match in jit_aggregate_equals.");

  switch (lhs.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_AGGREGATE_EQUALS_CASE, (JIT_DATA_TYPE_INFO))
    default:
      Fail("unreachable");
  }
}

void jit_assign(const JitTupleValue& from, const JitHashmapValue& to, const size_t to_index,
                JitRuntimeContext& context) {
  // jit_assign only supports identical data types. This is sufficient for the current JitAggregate implementation.
  // However, this function could easily be extended to support cross-data type assignment in a fashion similar to the
  // jit_compute function.
  DebugAssert(from.data_type() == to.data_type(), "Data types don't match in jit_assign.");

  if (to.is_nullable()) {
    const bool is_null = from.is_null(context);
    to.set_is_null(is_null, to_index, context);
    // The value is NULL - our work is done here.
    if (is_null) {
      return;
    }
  }

  switch (from.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_ASSIGN_CASE, (JIT_DATA_TYPE_INFO))
    default:
      break;
  }
}

size_t jit_grow_by_one(const JitHashmapValue& value, const JitVariantVector::InitialValue initial_value,
                       JitRuntimeContext& context) {
  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_GROW_BY_ONE_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return 0;
  }
}

// cleanup
#undef JIT_GET_ENUM_VALUE
#undef JIT_GET_DATA_TYPE
#undef JIT_HASH_CASE
#undef JIT_AGGREGATE_EQUALS_CASE
#undef JIT_ASSIGN_CASE
#undef JIT_GROW_BY_ONE_CASE
#undef JIT_IS_NULL_CASE
#undef JIT_IS_NOT_NULL_CASE

}  // namespace opossum
