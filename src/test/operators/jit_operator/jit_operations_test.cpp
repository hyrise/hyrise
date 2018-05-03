#include "operators/jit_operator/jit_operations.hpp"
#include "../../base_test.hpp"

namespace opossum {

class JitOperationsTest : public BaseTest {};

TEST_F(JitOperationsTest, ComputeResultType) {
  // We only test a selection of data type combinations and operations.
  const auto int_plus_int = jit_compute_type(jit_addition, DataType::Int, DataType::Int);
  EXPECT_EQ(int_plus_int, DataType::Int);

  const auto int_plus_long = jit_compute_type(jit_addition, DataType::Int, DataType::Long);
  EXPECT_EQ(int_plus_long, DataType::Long);

  const auto long_plus_int = jit_compute_type(jit_addition, DataType::Long, DataType::Int);
  EXPECT_EQ(long_plus_int, DataType::Long);

  const auto int_plus_float = jit_compute_type(jit_addition, DataType::Int, DataType::Float);
  EXPECT_EQ(int_plus_float, DataType::Float);

  const auto int_plus_double = jit_compute_type(jit_addition, DataType::Int, DataType::Double);
  EXPECT_EQ(int_plus_double, DataType::Double);

  const auto float_plus_double = jit_compute_type(jit_addition, DataType::Float, DataType::Double);
  EXPECT_EQ(float_plus_double, DataType::Double);

  const auto int_minus_int = jit_compute_type(jit_subtraction, DataType::Int, DataType::Int);
  EXPECT_EQ(int_minus_int, DataType::Int);

  const auto int_times_long = jit_compute_type(jit_multiplication, DataType::Int, DataType::Long);
  EXPECT_EQ(int_times_long, DataType::Long);

  const auto long_modulo_int = jit_compute_type(jit_modulo, DataType::Long, DataType::Int);
  EXPECT_EQ(long_modulo_int, DataType::Long);

  const auto int_raised_to_float = jit_compute_type(jit_power, DataType::Int, DataType::Float);
  EXPECT_EQ(int_raised_to_float, DataType::Double);

  const auto int_divided_by_double = jit_compute_type(jit_division, DataType::Int, DataType::Double);
  EXPECT_EQ(int_divided_by_double, DataType::Double);

  const auto float_times_double = jit_compute_type(jit_multiplication, DataType::Float, DataType::Double);
  EXPECT_EQ(float_times_double, DataType::Double);

  EXPECT_THROW(jit_compute_type(jit_addition, DataType::String, DataType::Int), std::logic_error);
  EXPECT_THROW(jit_compute_type(jit_modulo, DataType::Int, DataType::Float), std::logic_error);
}

TEST_F(JitOperationsTest, ArithmeticComputations) {
  // We only test a selection of data type combinations and operations.
  JitRuntimeContext context;
  context.tuple.resize(5);

  const JitTupleValue int_value{DataType::Int, false, 0};
  const JitTupleValue long_value{DataType::Long, false, 1};
  const JitTupleValue float_value{DataType::Float, false, 2};
  const JitTupleValue double_value{DataType::Double, false, 3};

  int_value.set<int32_t>(2, context);
  long_value.set<int64_t>(5l, context);
  float_value.set<float>(3.14f, context);
  double_value.set<double>(1.23, context);

  const JitTupleValue int_result_value{DataType::Int, false, 4};
  const JitTupleValue long_result_value{DataType::Long, false, 4};
  const JitTupleValue float_result_value{DataType::Float, false, 4};
  const JitTupleValue double_result_value{DataType::Double, false, 4};
  const JitTupleValue bool_result_value{DataType::Bool, false, 4};

  jit_compute(jit_addition, int_value, long_value, long_result_value, context);
  ASSERT_EQ(2 + 5l, long_result_value.get<int64_t>(context));

  jit_compute(jit_subtraction, float_value, long_value, float_result_value, context);
  ASSERT_EQ(3.14f - 5l, float_result_value.get<float>(context));

  jit_compute(jit_multiplication, double_value, int_value, double_result_value, context);
  ASSERT_EQ(1.23 * 2, double_result_value.get<double>(context));

  jit_compute(jit_division, int_value, float_value, float_result_value, context);
  ASSERT_EQ(2 / 3.14f, float_result_value.get<float>(context));

  jit_compute(jit_modulo, long_value, int_value, long_result_value, context);
  ASSERT_EQ(5l % 2, long_result_value.get<int64_t>(context));

  jit_compute(jit_power, double_value, float_value, double_result_value, context);
  ASSERT_EQ(std::pow(1.23, 3.14f), double_result_value.get<double>(context));
}

TEST_F(JitOperationsTest, Predicates) {
  JitRuntimeContext context;
  context.tuple.resize(5);

  const JitTupleValue int_1{DataType::Int, false, 0};
  const JitTupleValue int_2{DataType::Int, false, 1};
  const JitTupleValue float_1{DataType::Float, false, 2};
  const JitTupleValue float_2{DataType::Float, false, 3};
  const JitTupleValue result_value{DataType::Bool, false, 4};

  int_1.set<int32_t>(1, context);
  int_2.set<int32_t>(2, context);
  float_1.set<float>(1.0f, context);
  float_2.set<float>(2.0f, context);

  // GreaterThan
  jit_compute(jit_greater_than, int_1, int_2, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_greater_than, int_1, float_1, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_greater_than, float_2, int_1, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  // GreaterThanEquals
  jit_compute(jit_greater_than_equals, float_1, float_2, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_greater_than_equals, float_1, int_1, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_greater_than_equals, int_2, float_1, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  // LessThan
  jit_compute(jit_less_than, int_1, int_2, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_less_than, int_1, float_1, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_less_than, float_2, int_1, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  // LessThanEquals
  jit_compute(jit_less_than_equals, float_1, float_2, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_less_than_equals, float_1, int_1, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_less_than_equals, int_2, float_1, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  // Equals
  jit_compute(jit_equals, float_1, float_2, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_equals, float_1, int_1, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  // NotEquals
  jit_compute(jit_not_equals, int_1, int_2, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_not_equals, int_1, float_1, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));
}

TEST_F(JitOperationsTest, JitAnd) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleValue null_value{DataType::Bool, true, 0};
  const JitTupleValue true_value{DataType::Bool, false, 1};
  const JitTupleValue false_value{DataType::Bool, false, 2};
  const JitTupleValue result_value{DataType::Bool, true, 3};

  null_value.set_is_null(true, context);
  true_value.set(true, context);
  false_value.set(false, context);

  // Test of three-valued logic AND operation
  {
    jit_and(null_value, null_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_and(null_value, true_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_and(null_value, false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_and(true_value, null_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_and(true_value, true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_and(true_value, false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_and(false_value, null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_and(false_value, true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_and(false_value, false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }

  // Check that invalid data type combinations are rejected
  if (IS_DEBUG) {
    const JitTupleValue int_value{DataType::Int, false, 0};
    EXPECT_THROW(jit_and(true_value, int_value, result_value, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitOr) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleValue null_value{DataType::Bool, true, 0};
  const JitTupleValue true_value{DataType::Bool, false, 1};
  const JitTupleValue false_value{DataType::Bool, false, 2};
  const JitTupleValue result_value{DataType::Bool, true, 3};

  null_value.set_is_null(true, context);
  true_value.set(true, context);
  false_value.set(false, context);

  // Test of three-valued logic OR operation
  {
    jit_or(null_value, null_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_or(null_value, true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_or(null_value, false_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_or(true_value, null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_or(true_value, true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_or(true_value, false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_or(false_value, null_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_or(false_value, true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_or(false_value, false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }

  // Check that invalid data type combinations are rejected
  if (IS_DEBUG) {
    const JitTupleValue int_value{DataType::Int, false, 0};
    EXPECT_THROW(jit_or(true_value, int_value, result_value, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitNot) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleValue null_value{DataType::Bool, true, 0};
  const JitTupleValue true_value{DataType::Bool, false, 1};
  const JitTupleValue false_value{DataType::Bool, false, 2};
  const JitTupleValue result_value{DataType::Bool, true, 3};

  null_value.set_is_null(true, context);
  true_value.set(true, context);
  false_value.set(false, context);

  // Test of three-valued logic NOT operation
  {
    jit_not(null_value, result_value, context);
    EXPECT_TRUE(result_value.is_null(context));
  }
  {
    jit_not(true_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_not(false_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }

  // Check that invalid data type combinations are rejected
  if (IS_DEBUG) {
    const JitTupleValue int_value{DataType::Int, false, 0};
    EXPECT_THROW(jit_not(int_value, result_value, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitIs_Not_Null) {
  JitRuntimeContext context;
  context.tuple.resize(3);

  const JitTupleValue null_value{DataType::Bool, true, 0};
  const JitTupleValue non_null_value{DataType::Int, true, 1};
  const JitTupleValue result_value{DataType::Bool, false, 2};

  null_value.set_is_null(true, context);
  non_null_value.set_is_null(true, context);

  {
    jit_is_null(null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_is_not_null(null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
  {
    jit_is_null(non_null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_TRUE(result_value.get<bool>(context));
  }
  {
    jit_is_not_null(non_null_value, result_value, context);
    EXPECT_FALSE(result_value.is_null(context));
    EXPECT_FALSE(result_value.get<bool>(context));
  }
}

}  // namespace opossum
