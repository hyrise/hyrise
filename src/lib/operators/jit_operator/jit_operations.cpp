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

}  // namespace opossum
