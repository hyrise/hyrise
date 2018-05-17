#include "jit_operations.hpp"

namespace opossum {

#define JIT_GET_ENUM_VALUE(index, s) APPEND_ENUM_NAMESPACE(_, _, BOOST_PP_TUPLE_ELEM(3, 1, BOOST_PP_SEQ_ELEM(index, s)))
#define JIT_GET_DATA_TYPE(index, s) BOOST_PP_TUPLE_ELEM(3, 0, BOOST_PP_SEQ_ELEM(index, s))

#define JIT_COMPUTE_CASE(r, types)                                                                                   \
  case static_cast<uint8_t>(JIT_GET_ENUM_VALUE(0, types)) << 8 | static_cast<uint8_t>(JIT_GET_ENUM_VALUE(1, types)): \
    catching_func(lhs.get<JIT_GET_DATA_TYPE(0, types)>(context), rhs.get<JIT_GET_DATA_TYPE(1, types)>(context),      \
                  result);                                                                                           \
    break;

#define JIT_COMPUTE_TYPE_CASE(r, types)                                                                              \
  case static_cast<uint8_t>(JIT_GET_ENUM_VALUE(0, types)) << 8 | static_cast<uint8_t>(JIT_GET_ENUM_VALUE(1, types)): \
    return catching_func(JIT_GET_DATA_TYPE(0, types)(), JIT_GET_DATA_TYPE(1, types)());

#define JIT_AGGREGATE_COMPUTE_CASE(r, types)                                                                \
  case JIT_GET_ENUM_VALUE(0, types):                                                                        \
    rhs.set<JIT_GET_DATA_TYPE(0, types)>(op_func(lhs.get<JIT_GET_DATA_TYPE(0, types)>(context),             \
                                                 rhs.get<JIT_GET_DATA_TYPE(0, types)>(rhs_index, context)), \
                                         rhs_index, context);                                               \
    break;

#define JIT_HASH_CASE(r, types)                      \
  case JIT_GET_ENUM_VALUE(0, types):                 \
    return std::hash<JIT_GET_DATA_TYPE(0, types)>()( \
        context.tuple.get<JIT_GET_DATA_TYPE(0, types)>(value.tuple_index()));

#define JIT_EQUALS_CASE(r, types)    \
  case JIT_GET_ENUM_VALUE(0, types): \
    return lhs.get<JIT_GET_DATA_TYPE(0, types)>(context) == rhs.get<JIT_GET_DATA_TYPE(0, types)>(rhs_index, context);

#define JIT_ASSIGN_CASE(r, types)    \
  case JIT_GET_ENUM_VALUE(0, types): \
    return to.set<JIT_GET_DATA_TYPE(0, types)>(from.get<JIT_GET_DATA_TYPE(0, types)>(context), to_index, context);

#define JIT_GROW_BY_ONE_CASE(r, types) \
  case JIT_GET_ENUM_VALUE(0, types):   \
    return context.hashmap.values[value.column_index()].grow_by_one<JIT_GET_DATA_TYPE(0, types)>(initial_value);

void jit_not(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context) {
  DebugAssert(lhs.data_type() == DataType::Bool && result.data_type() == DataType::Bool, "invalid type for operation");
  result.set<bool>(!lhs.get<bool>(context), context);
  result.set_is_null(lhs.is_null(context), context);
}

void jit_and(const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
             JitRuntimeContext& context) {
  DebugAssert(
      lhs.data_type() == DataType::Bool && rhs.data_type() == DataType::Bool && result.data_type() == DataType::Bool,
      "invalid type for operation");

  // three-valued logic AND
  if (lhs.is_null(context)) {
    result.set<bool>(false, context);
    result.set_is_null(rhs.is_null(context) || rhs.get<bool>(context), context);
  } else {
    result.set<bool>(lhs.get<bool>(context) && rhs.get<bool>(context), context);
    result.set_is_null(lhs.get<bool>(context) && rhs.is_null(context), context);
  }
}

void jit_or(const JitTupleValue& lhs, const JitTupleValue& rhs, const JitTupleValue& result,
            JitRuntimeContext& context) {
  DebugAssert(
      lhs.data_type() == DataType::Bool && rhs.data_type() == DataType::Bool && result.data_type() == DataType::Bool,
      "invalid type for operation");

  // three-valued logic OR
  if (lhs.is_null(context)) {
    result.set<bool>(true, context);
    result.set_is_null(rhs.is_null(context) || !rhs.get<bool>(context), context);
  } else {
    result.set<bool>(lhs.get<bool>(context) || rhs.get<bool>(context), context);
    result.set_is_null(!lhs.get<bool>(context) && rhs.is_null(context), context);
  }
}

void jit_is_null(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context) {
  DebugAssert(result.data_type() == DataType::Bool, "invalid type for operation");
  result.set_is_null(false, context);
  result.set<bool>(lhs.is_null(context), context);
}

void jit_is_not_null(const JitTupleValue& lhs, const JitTupleValue& result, JitRuntimeContext& context) {
  DebugAssert(result.data_type() == DataType::Bool, "invalid type for operation");
  result.set_is_null(false, context);
  result.set<bool>(!lhs.is_null(context), context);
}

uint64_t jit_hash(const JitTupleValue& value, JitRuntimeContext& context) {
  if (value.is_null(context)) {
    return 0;
  }

  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_HASH_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return 0;
  }
};

bool jit_aggregate_equals(const JitTupleValue& lhs, const JitHashmapValue& rhs, const size_t rhs_index,
                          JitRuntimeContext& context) {
  if (lhs.is_null(context) && rhs.is_null(rhs_index, context)) {
    return true;
  }
  if (lhs.is_null(context) || rhs.is_null(rhs_index, context)) {
    return false;
  }

  switch (lhs.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_EQUALS_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return true;
  }
};

void jit_assign(const JitTupleValue& from, const JitHashmapValue& to, const size_t to_index,
                JitRuntimeContext& context) {
  if (to.is_nullable()) {
    const bool is_null = from.is_null(context);
    to.set_is_null(is_null, to_index, context);
    if (is_null) {
      return;
    }
  }

  switch (from.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_ASSIGN_CASE, (JIT_DATA_TYPE_INFO))
    default:
      break;
  }
};

size_t jit_grow_by_one(const JitHashmapValue& value, const JitVariantVector::InitialValue initial_value,
                       JitRuntimeContext& context) {
  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_GROW_BY_ONE_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return 0;
  }
}

//cleanup
#undef JIT_GET_ENUM_VALUE
#undef JIT_GET_DATA_TYPE
#undef JIT_HASH_CASE
#undef JIT_EQUALS_CASE
#undef JIT_ASSIGN_CASE
#undef JIT_GROW_BY_ONE_CASE

}  // namespace opossum
