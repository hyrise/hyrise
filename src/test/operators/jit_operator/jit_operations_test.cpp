#include "base_test.hpp"
#include "operators/jit_operator/jit_operations.hpp"

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
  context.tuple.resize(8);

  const JitTupleValue int_1{DataType::Int, false, 0};
  const JitTupleValue int_2{DataType::Int, false, 1};
  const JitTupleValue float_1{DataType::Float, false, 2};
  const JitTupleValue float_2{DataType::Float, false, 3};
  const JitTupleValue string_1{DataType::String, false, 4};
  const JitTupleValue string_2{DataType::String, false, 5};
  const JitTupleValue string_3{DataType::String, false, 6};
  const JitTupleValue result_value{DataType::Bool, false, 7};

  int_1.set<int32_t>(1, context);
  int_2.set<int32_t>(2, context);
  float_1.set<float>(1.0f, context);
  float_2.set<float>(2.0f, context);
  string_1.set<std::string>("hello", context);
  string_2.set<std::string>("h%", context);
  string_3.set<std::string>("H%", context);

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

  // LIKE
  jit_compute(jit_like, string_1, string_2, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));

  jit_compute(jit_like, string_1, string_3, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  // NOT LIKE
  jit_compute(jit_not_like, string_1, string_2, result_value, context);
  ASSERT_FALSE(result_value.get<bool>(context));

  jit_compute(jit_not_like, string_1, string_3, result_value, context);
  ASSERT_TRUE(result_value.get<bool>(context));
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

TEST_F(JitOperationsTest, JitHash) {
  JitRuntimeContext context;
  context.tuple.resize(1);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value) {
    using ValueType = decltype(value);
    const JitTupleValue tuple_value{data_type, false, 0};
    tuple_value.set(value, context);
    const auto expected_hash = std::hash<ValueType>()(value);
    const auto actual_hash = jit_hash(tuple_value, context);
    EXPECT_EQ(expected_hash, actual_hash);
  };

  typed_test(DataType::Int, static_cast<int32_t>(std::rand()));
  typed_test(DataType::Long, static_cast<int64_t>(std::rand()));
  typed_test(DataType::Float, static_cast<float>(std::rand()));
  typed_test(DataType::Double, static_cast<double>(std::rand()));
  typed_test(DataType::String, std::string("some string"));

  // Check jit_hash with NULL value
  const JitTupleValue tuple_value{DataType::Int, true, 0};
  tuple_value.set_is_null(true, context);
  EXPECT_EQ(jit_hash(tuple_value, context), 0u);
}

TEST_F(JitOperationsTest, JitAggregateEquals) {
  JitRuntimeContext context;
  context.tuple.resize(3);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(2);

  const JitTupleValue tuple_value_1{DataType::Int, false, 0};
  const JitTupleValue tuple_value_2{DataType::Int, false, 1};
  const JitTupleValue tuple_value_3{DataType::Int, true, 2};

  const JitHashmapValue hashmap_value_1{DataType::Int, false, 0};
  const JitHashmapValue hashmap_count_for_avg{DataType::Int, true, 0};

  const auto value = static_cast<int32_t>(std::rand() % 1000);
  tuple_value_1.set<int32_t>(value, context);
  tuple_value_2.set<int32_t>(value + 1, context);
  tuple_value_3.set_is_null(true, context);

  hashmap_value_1.set<int32_t>(value, 0, context);
  hashmap_count_for_avg.set_is_null(true, 1, context);

  // Equal values
  EXPECT_TRUE(jit_aggregate_equals(tuple_value_1, hashmap_value_1, 0, context));

  // Unequal values
  EXPECT_FALSE(jit_aggregate_equals(tuple_value_2, hashmap_value_1, 0, context));

  // Comparing value to NULL
  EXPECT_FALSE(jit_aggregate_equals(tuple_value_1, hashmap_count_for_avg, 1, context));
  EXPECT_FALSE(jit_aggregate_equals(tuple_value_3, hashmap_value_1, 0, context));

  // Comparing NULL to NULL
  EXPECT_TRUE(jit_aggregate_equals(tuple_value_3, hashmap_count_for_avg, 1, context));
}

TEST_F(JitOperationsTest, JitAssign) {
  JitRuntimeContext context;
  context.tuple.resize(4);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(1);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value) {
    using ValueType = decltype(value);
    const JitTupleValue tuple_value{data_type, true, 0};
    const JitHashmapValue hashmap_value{data_type, true, 0};

    tuple_value.set(value, context);
    jit_assign(tuple_value, hashmap_value, 0, context);

    EXPECT_FALSE(hashmap_value.is_null(0, context));
    EXPECT_EQ(hashmap_value.get<ValueType>(0, context), value);
  };

  typed_test(DataType::Int, static_cast<int32_t>(std::rand()));
  typed_test(DataType::Long, static_cast<int64_t>(std::rand()));
  typed_test(DataType::Float, static_cast<float>(std::rand()));
  typed_test(DataType::Double, static_cast<double>(std::rand()));
  typed_test(DataType::String, std::string("some string"));

  const JitTupleValue tuple_value{DataType::Int, true, 0};
  const JitHashmapValue hashmap_value{DataType::Int, true, 0};

  // Check jit_assign with NULL value
  tuple_value.set_is_null(true, context);
  jit_assign(tuple_value, hashmap_value, 0, context);

  EXPECT_TRUE(hashmap_value.is_null(0, context));
}

TEST_F(JitOperationsTest, JitGrowByOne) {
  JitRuntimeContext context;
  context.hashmap.columns.resize(5);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value, size_t column_index) {
    using ValueType = decltype(value);
    JitHashmapValue hashmap_value{data_type, false, column_index};

    // The vector should be empty initially
    EXPECT_EQ(context.hashmap.columns[0].get_vector<ValueType>().size(), 0u);

    // Add one of each possible initial values to the vector
    jit_grow_by_one(hashmap_value, JitVariantVector::InitialValue::Zero, context);
    jit_grow_by_one(hashmap_value, JitVariantVector::InitialValue::MaxValue, context);
    jit_grow_by_one(hashmap_value, JitVariantVector::InitialValue::MinValue, context);

    // Check, that the vector contains the correct three values
    EXPECT_EQ(context.hashmap.columns[column_index].get_vector<ValueType>().size(), 3u);
    EXPECT_EQ(context.hashmap.columns[column_index].get_vector<ValueType>()[0], ValueType{});
    EXPECT_EQ(context.hashmap.columns[column_index].get_vector<ValueType>()[1], std::numeric_limits<ValueType>::max());
    EXPECT_EQ(context.hashmap.columns[column_index].get_vector<ValueType>()[2], std::numeric_limits<ValueType>::min());
  };

  typed_test(DataType::Int, int32_t{}, 0);
  typed_test(DataType::Long, int64_t{}, 1);
  typed_test(DataType::Float, float{}, 2);
  typed_test(DataType::Double, double{}, 3);
  typed_test(DataType::String, std::string{}, 4);
}

TEST_F(JitOperationsTest, JitAggregateCompute) {
  JitRuntimeContext context;
  context.tuple.resize(3);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(1);

  // jit_increment operation
  {
    const JitTupleValue tuple_value{DataType::String, false, 0};
    const JitTupleValue null_tuple_value{DataType::String, true, 1};
    const JitHashmapValue hashmap_value{DataType::Long, false, 0};

    tuple_value.set<std::string>("some string", context);
    null_tuple_value.set_is_null(true, context);
    hashmap_value.set<int64_t>(5, 0, context);

    ASSERT_EQ(5, hashmap_value.get<int64_t>(0, context));
    jit_aggregate_compute(jit_increment, tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(6, hashmap_value.get<int64_t>(0, context));
    jit_aggregate_compute(jit_increment, tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(7, hashmap_value.get<int64_t>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_increment, null_tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(7, hashmap_value.get<int64_t>(0, context));
  }

  // jit_maximum operation
  {
    const JitTupleValue tuple_value_1{DataType::Float, false, 0};
    const JitTupleValue tuple_value_2{DataType::Float, false, 1};
    const JitTupleValue null_tuple_value{DataType::Float, true, 2};
    const JitHashmapValue hashmap_value{DataType::Float, false, 0};

    tuple_value_1.set<float>(1.234f, context);
    tuple_value_2.set<float>(3.456f, context);
    null_tuple_value.set_is_null(true, context);
    hashmap_value.set<float>(2.345f, 0, context);

    ASSERT_EQ(2.345f, hashmap_value.get<float>(0, context));
    jit_aggregate_compute(jit_maximum, tuple_value_1, hashmap_value, 0, context);
    ASSERT_EQ(2.345f, hashmap_value.get<float>(0, context));
    jit_aggregate_compute(jit_maximum, tuple_value_2, hashmap_value, 0, context);
    ASSERT_EQ(3.456f, hashmap_value.get<float>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_maximum, null_tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(3.456f, hashmap_value.get<float>(0, context));
  }

  // jit_minimum operation
  {
    const JitTupleValue tuple_value_1{DataType::Double, false, 0};
    const JitTupleValue tuple_value_2{DataType::Double, false, 1};
    const JitTupleValue null_tuple_value{DataType::Double, true, 2};
    const JitHashmapValue hashmap_value{DataType::Double, false, 0};

    tuple_value_1.set<double>(3.456f, context);
    tuple_value_2.set<double>(1.234f, context);
    null_tuple_value.set_is_null(true, context);
    hashmap_value.set<double>(2.345f, 0, context);

    ASSERT_EQ(2.345f, hashmap_value.get<double>(0, context));
    jit_aggregate_compute(jit_minimum, tuple_value_1, hashmap_value, 0, context);
    ASSERT_EQ(2.345f, hashmap_value.get<double>(0, context));
    jit_aggregate_compute(jit_minimum, tuple_value_2, hashmap_value, 0, context);
    ASSERT_EQ(1.234f, hashmap_value.get<double>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_minimum, null_tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(1.234f, hashmap_value.get<double>(0, context));
  }

  // jit_addition operation
  {
    const JitTupleValue tuple_value_1{DataType::Long, false, 0};
    const JitTupleValue tuple_value_2{DataType::Long, false, 1};
    const JitTupleValue null_tuple_value{DataType::Long, true, 2};
    const JitHashmapValue hashmap_value{DataType::Long, false, 0};

    tuple_value_1.set<int64_t>(5, context);
    tuple_value_2.set<int64_t>(-10, context);
    null_tuple_value.set_is_null(true, context);
    hashmap_value.set<int64_t>(100, 0, context);

    ASSERT_EQ(100, hashmap_value.get<int64_t>(0, context));
    jit_aggregate_compute(jit_addition, tuple_value_1, hashmap_value, 0, context);
    ASSERT_EQ(105, hashmap_value.get<int64_t>(0, context));
    jit_aggregate_compute(jit_addition, tuple_value_2, hashmap_value, 0, context);
    ASSERT_EQ(95, hashmap_value.get<int64_t>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_addition, null_tuple_value, hashmap_value, 0, context);
    ASSERT_EQ(95, hashmap_value.get<int64_t>(0, context));
  }
}

}  // namespace opossum
