#include "jit_operations.hpp"

namespace opossum {

#define JIT_HASH_CASE(r, types)                      \
  case JIT_GET_ENUM_VALUE(0, types):                 \
    return std::hash<JIT_GET_DATA_TYPE(0, types)>()( \
        context.tuple.get<JIT_GET_DATA_TYPE(0, types)>(tuple_entry.tuple_index()));

#define JIT_AGGREGATE_EQUALS_CASE(r, types) \
  case JIT_GET_ENUM_VALUE(0, types):        \
    return lhs.get<JIT_GET_DATA_TYPE(0, types)>(context) == rhs.get<JIT_GET_DATA_TYPE(0, types)>(rhs_index, context);

#define JIT_ASSIGN_CASE(r, types)    \
  case JIT_GET_ENUM_VALUE(0, types): \
    return to.set<JIT_GET_DATA_TYPE(0, types)>(from.get<JIT_GET_DATA_TYPE(0, types)>(context), to_index, context);

#define JIT_GROW_BY_ONE_CASE(r, types)                                                                     \
  case JIT_GET_ENUM_VALUE(0, types):                                                                       \
    return context.hashmap.columns[hashmap_entry.column_index()].grow_by_one<JIT_GET_DATA_TYPE(0, types)>( \
        initial_value);

#define JIT_IS_NULL_CASE(r, types)                                               \
  case JIT_GET_ENUM_VALUE(0, types): {                                           \
    const auto result = left_side.compute<JIT_GET_DATA_TYPE(0, types)>(context); \
    return !result.has_value();                                                  \
  }

#define JIT_IS_NOT_NULL_CASE(r, types)                                           \
  case JIT_GET_ENUM_VALUE(0, types): {                                           \
    const auto result = left_side.compute<JIT_GET_DATA_TYPE(0, types)>(context); \
    return result.has_value();                                                   \
  }

std::optional<bool> jit_not(const JitExpression& left_side, JitRuntimeContext& context) {
  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert(
      (left_side.result_entry().data_type() == DataType::Bool || left_side.result_entry().data_type() == DataType::Int),
      "invalid type for jit operation not");
  const auto value = left_side.compute<bool>(context);
  if (left_side.result_entry().is_nullable() && !value.has_value()) {
    return std::nullopt;
  } else {
    return !value.value();
  }
}

std::optional<bool> jit_and(const JitExpression& left_side, const JitExpression& right_side,
                            JitRuntimeContext& context) {
  // three-valued logic AND
  // Short-circuit evaluation is used to skip the evaluation of the right side if the value of the left side is not null
  // and false.

  // Get left and right operand types, the actual operand values are computed later
  const auto left_entry = left_side.result_entry();
  const auto right_entry = right_side.result_entry();

  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert((left_entry.data_type() == DataType::Bool || left_entry.data_type() == DataType::Int) &&
                  (right_entry.data_type() == DataType::Bool || right_entry.data_type() == DataType::Int),
              "invalid type for jit operation and");

  const auto left_result = left_side.compute<bool>(context);
  // Computation of right hand side can be pruned if left result is false and not null
  if (!left_entry.is_nullable() || left_result.has_value()) {  // Left result is not null
    if (!left_result.value()) {                                // Left result is false
      return false;
    }
  }

  // Left result is null or true
  const auto right_result = right_side.compute<bool>(context);
  if (left_entry.is_nullable() && !left_result.has_value()) {  // Left result is null
    // Right result is null or true
    if ((right_entry.is_nullable() && !right_result.has_value()) || right_result.value()) {
      return std::nullopt;
    } else {  // Right result is false
      return false;
    }
  }

  // Left result is false and not null
  if (right_entry.is_nullable() && !right_result.has_value()) {
    return std::nullopt;
  } else {
    return right_result.value();
  }
}

std::optional<bool> jit_or(const JitExpression& left_side, const JitExpression& right_side,
                           JitRuntimeContext& context) {
  // three-valued logic OR
  // Short-circuit evaluation is used to skip the evaluation of the right side if the value of the left side is not null
  // and true.

  // Get left and right operand types, the actual operand values are computed later
  const auto left_entry = left_side.result_entry();
  const auto right_entry = right_side.result_entry();

  // If the input value is computed by a non-jit operator, its data type is int but it can be read as a bool value.
  DebugAssert((left_entry.data_type() == DataType::Bool || left_entry.data_type() == DataType::Int) &&
                  (right_entry.data_type() == DataType::Bool || right_entry.data_type() == DataType::Int),
              "invalid type for jit operation or");

  const auto left_result = left_side.compute<bool>(context);
  // Computation of right hand side can be pruned if left result is true and not null
  if (!left_entry.is_nullable() || left_result.has_value()) {  // Left result is not null
    if (left_result.value()) {                                 // Left result is true
      return true;
    }
  }

  // Left result is null or false
  const auto right_result = right_side.compute<bool>(context);
  if (left_entry.is_nullable() && !left_result.has_value()) {  // Left result is null
    // Right result is null or false
    if ((right_entry.is_nullable() && !right_result.has_value()) || !right_result.value()) {
      return std::nullopt;
    } else {  // Right result is true
      return true;
    }
  }

  // Left result is false and not null
  if (right_entry.is_nullable() && !right_result.has_value()) {
    return std::nullopt;
  } else {
    return right_result.value();
  }
}

// TODO(anyone) State Machine is currently build for every comparison. It should be build only once.
bool jit_like(const pmr_string& a, const pmr_string& b) {
  const auto regex_string = LikeMatcher::sql_like_to_regex(b);
  const auto regex = std::regex{regex_string};
  return std::regex_match(a, regex);
}

// TODO(anyone) State Machine is currently build for every comparison. It should be build only once.
bool jit_not_like(const pmr_string& a, const pmr_string& b) {
  const auto regex_string = LikeMatcher::sql_like_to_regex(b);
  const auto regex = std::regex{regex_string};
  return !std::regex_match(a, regex);
}

std::optional<bool> jit_is_null(const JitExpression& left_side, JitRuntimeContext& context) {
  // switch and macros required to call compute<ResultValueType>() on left_side with the correct ResultValueType
  // template parameter for each data type.
  switch (left_side.result_entry().data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_IS_NULL_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      return true;
  }
}

std::optional<bool> jit_is_not_null(const JitExpression& left_side, JitRuntimeContext& context) {
  // switch and macros required to call compute<ResultValueType>() on left_side with the correct ResultValueType
  // template parameter for each data type.
  switch (left_side.result_entry().data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_IS_NOT_NULL_CASE, (JIT_DATA_TYPE_INFO))
    case DataType::Null:
      return false;
  }
}

uint64_t jit_hash(const JitTupleEntry& tuple_entry, JitRuntimeContext& context) {
  // NULL values hash to 0.
  if (tuple_entry.is_null(context)) {
    return 0;
  }

  // For all other values the hash is computed by the corresponding std::hash function
  switch (tuple_entry.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_HASH_CASE, (JIT_DATA_TYPE_INFO))
    default:
      Fail("unreachable");
  }
}

bool jit_aggregate_equals(const JitTupleEntry& lhs, const JitHashmapEntry& rhs, const size_t rhs_index,
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

void jit_assign(const JitTupleEntry& from, const JitHashmapEntry& to, const size_t to_index,
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

size_t jit_grow_by_one(const JitHashmapEntry& hashmap_entry, const JitVariantVector::InitialValue initial_value,
                       JitRuntimeContext& context) {
  switch (hashmap_entry.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_GROW_BY_ONE_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return 0;
  }
}

// cleanup
#undef JIT_HASH_CASE
#undef JIT_AGGREGATE_EQUALS_CASE
#undef JIT_ASSIGN_CASE
#undef JIT_GROW_BY_ONE_CASE
#undef JIT_IS_NULL_CASE
#undef JIT_IS_NOT_NULL_CASE

}  // namespace opossum
