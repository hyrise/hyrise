#include "jit_operations.hpp"

namespace opossum {

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
  if (value.is_null(context)) { return 0; }

  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_HASH_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return 0;
  }
};

bool jit_aggregate_equals(const JitTupleValue& lhs, const JitHashmapValue& rhs, const size_t rhs_index, JitRuntimeContext& context) {
  if (lhs.is_null(context) && rhs.is_null(rhs_index, context)) { return true; }
  if (lhs.is_null(context) || rhs.is_null(rhs_index, context)) { return false; }

  switch (lhs.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_EQUALS_CASE, (JIT_DATA_TYPE_INFO))
    default:
      return true;
  }
};

void jit_assign(const JitTupleValue& from, const JitHashmapValue& to, const size_t to_index, JitRuntimeContext& context) {
  if (to.is_nullable()) {
    const bool is_null = from.is_null(context);
    to.set_is_null(is_null, to_index, context);
    if (is_null) { return; }
  }

  // This lambda calls the op_func (a lambda that performs the actual computation) with type arguments and stores
  // the result.
  const auto store_result_wrapper = [&](const auto& typed_lhs, const auto& typed_rhs, auto& result) -> decltype(typed_lhs = typed_rhs, void()) {
    to.set<JIT_GET_DATA_TYPE(1, types)>(from.get<JIT_GET_DATA_TYPE(0, types)>(context), to_index, context);
  };

  const auto catching_func = InvalidTypeCatcher<decltype(store_result_wrapper), void>(store_result_wrapper);

  const auto combined_types = static_cast<uint8_t>(from.data_type()) << 8 | static_cast<uint8_t>(to.data_type());
  switch (combined_types) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_ASSIGN_CASE, (JIT_DATA_TYPE_INFO)(JIT_DATA_TYPE_INFO))
    default:
      Fail("unreachable");
  }
};

size_t jit_grow_by_one(const JitHashmapValue& value, JitRuntimeContext& context) {
  switch (value.data_type()) {
    BOOST_PP_SEQ_FOR_EACH_PRODUCT(JIT_GROW_BY_ONE_CASE, (JIT_DATA_TYPE_INFO))
    default:
      Fail("unreachable");
  }
}

}  // namespace opossum
