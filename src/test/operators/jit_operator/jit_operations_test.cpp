#include "base_test.hpp"
#include "operators/jit_operator/jit_operations.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"

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

  const JitTupleEntry int_tuple_entry{DataType::Int, false, 0};
  const JitTupleEntry long_tuple_entry{DataType::Long, false, 1};
  const JitTupleEntry float_tuple_entry{DataType::Float, false, 2};
  const JitTupleEntry double_tuple_entry{DataType::Double, false, 3};

  int_tuple_entry.set<int32_t>(2, context);
  long_tuple_entry.set<int64_t>(5l, context);
  float_tuple_entry.set<float>(3.14f, context);
  double_tuple_entry.set<double>(1.23, context);

  const JitExpression int_value_expression{int_tuple_entry};
  const JitExpression long_value_expression{long_tuple_entry};
  const JitExpression float_value_expression{float_tuple_entry};
  const JitExpression double_value_expression{double_tuple_entry};

  auto result_value_1 = jit_compute<int64_t>(jit_addition, int_value_expression, long_value_expression, context);
  ASSERT_EQ(2 + 5l, result_value_1.value());

  auto result_value_2 = jit_compute<float>(jit_subtraction, float_value_expression, long_value_expression, context);
  ASSERT_EQ(3.14f - 5l, result_value_2.value());

  auto result_value_3 = jit_compute<double>(jit_multiplication, double_value_expression, int_value_expression, context);
  ASSERT_EQ(1.23 * 2, result_value_3.value());

  auto result_value_4 = jit_compute<float>(jit_division, int_value_expression, float_value_expression, context);
  ASSERT_EQ(2 / 3.14f, result_value_4.value());

  auto result_value_5 = jit_compute<int64_t>(jit_modulo, long_value_expression, int_value_expression, context);
  ASSERT_EQ(5l % 2, result_value_5.value());

  auto result_value_6 = jit_compute<double>(jit_power, double_value_expression, float_value_expression, context);
  ASSERT_EQ(std::pow(1.23, 3.14f), result_value_6.value());
}

TEST_F(JitOperationsTest, Predicates) {
  JitRuntimeContext context;
  context.tuple.resize(8);

  const JitTupleEntry int_1{DataType::Int, false, 0};
  const JitTupleEntry int_2{DataType::Int, false, 1};
  const JitTupleEntry float_1{DataType::Float, false, 2};
  const JitTupleEntry float_2{DataType::Float, false, 3};
  const JitTupleEntry string_1{DataType::String, false, 4};
  const JitTupleEntry string_2{DataType::String, false, 5};
  const JitTupleEntry string_3{DataType::String, false, 6};

  int_1.set<int32_t>(1, context);
  int_2.set<int32_t>(2, context);
  float_1.set<float>(1.0f, context);
  float_2.set<float>(2.0f, context);
  string_1.set<pmr_string>("hello", context);
  string_2.set<pmr_string>("h%", context);
  string_3.set<pmr_string>("H%", context);

  const JitExpression int_1_expression{int_1};
  const JitExpression int_2_expression{int_2};
  const JitExpression float_1_expression{float_1};
  const JitExpression float_2_expression{float_2};
  const JitExpression string_1_expression{string_1};
  const JitExpression string_2_expression{string_2};
  const JitExpression string_3_expression{string_3};

  std::optional<bool> result_value;

  // GreaterThan
  result_value = jit_compute<bool>(jit_greater_than, int_1_expression, int_2_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_greater_than, int_1_expression, float_1_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_greater_than, float_2_expression, int_1_expression, context);
  ASSERT_TRUE(result_value.value());

  // GreaterThanEquals
  result_value = jit_compute<bool>(jit_greater_than_equals, float_1_expression, float_2_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_greater_than_equals, float_1_expression, int_1_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_greater_than_equals, int_2_expression, float_1_expression, context);
  ASSERT_TRUE(result_value.value());

  // LessThan
  result_value = jit_compute<bool>(jit_less_than, int_1_expression, int_2_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_less_than, int_1_expression, float_1_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_less_than, float_2_expression, int_1_expression, context);
  ASSERT_FALSE(result_value.value());

  // LessThanEquals
  result_value = jit_compute<bool>(jit_less_than_equals, float_1_expression, float_2_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_less_than_equals, float_1_expression, int_1_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_less_than_equals, int_2_expression, float_1_expression, context);
  ASSERT_FALSE(result_value.value());

  // Equals
  result_value = jit_compute<bool>(jit_equals, float_1_expression, float_2_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_equals, float_1_expression, int_1_expression, context);
  ASSERT_TRUE(result_value.value());

  // NotEquals
  result_value = jit_compute<bool>(jit_not_equals, int_1_expression, int_2_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_not_equals, int_1_expression, float_1_expression, context);
  ASSERT_FALSE(result_value.value());

  // LIKE
  result_value = jit_compute<bool>(jit_like, string_1_expression, string_2_expression, context);
  ASSERT_TRUE(result_value.value());

  result_value = jit_compute<bool>(jit_like, string_1_expression, string_3_expression, context);
  ASSERT_FALSE(result_value.value());

  // NOT LIKE
  result_value = jit_compute<bool>(jit_not_like, string_1_expression, string_2_expression, context);
  ASSERT_FALSE(result_value.value());

  result_value = jit_compute<bool>(jit_not_like, string_1_expression, string_3_expression, context);
  ASSERT_TRUE(result_value.value());
}

TEST_F(JitOperationsTest, JitAnd) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleEntry null_tuple_entry{DataType::Bool, true, 0};
  const JitTupleEntry true_tuple_entry{DataType::Bool, false, 1};
  const JitTupleEntry false_tuple_entry{DataType::Bool, false, 2};

  null_tuple_entry.set_is_null(true, context);
  true_tuple_entry.set(true, context);
  false_tuple_entry.set(false, context);

  const JitExpression null_value_expression{null_tuple_entry};
  const JitExpression true_value_expression{true_tuple_entry};
  const JitExpression false_value_expression{false_tuple_entry};

  std::optional<bool> result_value;

  // Test of three-valued logic AND operation
  {
    result_value = jit_and(null_value_expression, null_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_and(null_value_expression, true_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_and(null_value_expression, false_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    result_value = jit_and(true_value_expression, null_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_and(true_value_expression, true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_and(true_value_expression, false_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    result_value = jit_and(false_value_expression, null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    result_value = jit_and(false_value_expression, true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    result_value = jit_and(false_value_expression, false_value_expression, context);
    EXPECT_FALSE(result_value.value());
    EXPECT_FALSE(result_value.value());
  }

  // Check that invalid data type combinations are rejected
  if (HYRISE_DEBUG) {
    const JitTupleEntry long_tuple_entry{DataType::Long, false, 0};
    const JitExpression long_value_expression{long_tuple_entry};
    EXPECT_THROW(jit_and(true_value_expression, long_value_expression, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitOr) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleEntry null_tuple_entry{DataType::Bool, true, 0};
  const JitTupleEntry true_tuple_entry{DataType::Bool, false, 1};
  const JitTupleEntry false_tuple_entry{DataType::Bool, false, 2};

  null_tuple_entry.set_is_null(true, context);
  true_tuple_entry.set(true, context);
  false_tuple_entry.set(false, context);

  const JitExpression null_value_expression{null_tuple_entry};
  const JitExpression true_value_expression{true_tuple_entry};
  const JitExpression false_value_expression{false_tuple_entry};

  std::optional<bool> result_value;

  // Test of three-valued logic OR operation
  {
    result_value = jit_or(null_value_expression, null_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_or(null_value_expression, true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_or(null_value_expression, false_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_or(true_value_expression, null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_or(true_value_expression, true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_or(true_value_expression, false_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_or(false_value_expression, null_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_or(false_value_expression, true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    result_value = jit_or(false_value_expression, false_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }

  // Check that invalid data type combinations are rejected
  if (HYRISE_DEBUG) {
    const JitTupleEntry long_tuple_entry{DataType::Long, false, 0};
    const JitExpression long_value_expression{long_tuple_entry};
    EXPECT_THROW(jit_or(true_value_expression, long_value_expression, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitNot) {
  JitRuntimeContext context;
  context.tuple.resize(4);

  const JitTupleEntry null_tuple_entry{DataType::Bool, true, 0};
  const JitTupleEntry true_tuple_entry{DataType::Bool, false, 1};
  const JitTupleEntry false_tuple_entry{DataType::Bool, false, 2};

  null_tuple_entry.set_is_null(true, context);
  true_tuple_entry.set(true, context);
  false_tuple_entry.set(false, context);

  const JitExpression null_value_expression{null_tuple_entry};
  const JitExpression true_value_expression{true_tuple_entry};
  const JitExpression false_value_expression{false_tuple_entry};

  std::optional<bool> result_value;

  // Test of three-valued logic NOT operation
  {
    result_value = jit_not(null_value_expression, context);
    EXPECT_FALSE(result_value.has_value());
  }
  {
    result_value = jit_not(true_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    result_value = jit_not(false_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }

  // Check that invalid data type combinations are rejected
  if (HYRISE_DEBUG) {
    const JitTupleEntry long_tuple_entry{DataType::Long, false, 0};
    const JitExpression long_value_expression{long_tuple_entry};
    EXPECT_THROW(jit_not(long_value_expression, context), std::logic_error);
  }
}

TEST_F(JitOperationsTest, JitIs_Not_Null) {
  JitRuntimeContext context;
  context.tuple.resize(3);

  const JitTupleEntry null_tuple_entry{DataType::Bool, true, 0};
  const JitTupleEntry non_null_tuple_entry{DataType::Int, true, 1};

  null_tuple_entry.set_is_null(true, context);
  non_null_tuple_entry.set_is_null(true, context);

  const JitExpression null_value_expression{null_tuple_entry};
  const JitExpression non_null_value_expression{non_null_tuple_entry};

  std::optional<bool> result_value;

  {
    // null value with is null check
    result_value = jit_is_null(null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    // null value with is not null check
    result_value = jit_is_not_null(null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
  {
    // non null value with is null check
    result_value = jit_is_null(non_null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_TRUE(result_value.value());
  }
  {
    // non null value with is not null check
    result_value = jit_is_not_null(non_null_value_expression, context);
    EXPECT_TRUE(result_value.has_value());
    EXPECT_FALSE(result_value.value());
  }
}

TEST_F(JitOperationsTest, JitHash) {
  JitRuntimeContext context;
  context.tuple.resize(1);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value) {
    using ValueType = decltype(value);
    const JitTupleEntry tuple_entry{data_type, false, 0};
    tuple_entry.set(value, context);
    const auto expected_hash = std::hash<ValueType>()(value);
    const auto actual_hash = jit_hash(tuple_entry, context);
    EXPECT_EQ(expected_hash, actual_hash);
  };

  typed_test(DataType::Int, static_cast<int32_t>(std::rand()));
  typed_test(DataType::Long, static_cast<int64_t>(std::rand()));
  typed_test(DataType::Float, static_cast<float>(std::rand()));
  typed_test(DataType::Double, static_cast<double>(std::rand()));
  typed_test(DataType::String, pmr_string("some string"));

  // Check jit_hash with NULL value
  const JitTupleEntry tuple_entry{DataType::Int, true, 0};
  tuple_entry.set_is_null(true, context);
  EXPECT_EQ(jit_hash(tuple_entry, context), 0u);
}

TEST_F(JitOperationsTest, JitAggregateEquals) {
  JitRuntimeContext context;
  context.tuple.resize(3);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(2);

  const JitTupleEntry tuple_entry_1{DataType::Int, false, 0};
  const JitTupleEntry tuple_entry_2{DataType::Int, false, 1};
  const JitTupleEntry tuple_entry_3{DataType::Int, true, 2};

  const JitHashmapEntry hashmap_entry_1{DataType::Int, false, 0};
  const JitHashmapEntry hashmap_count_for_avg{DataType::Int, true, 0};

  const auto value = static_cast<int32_t>(std::rand() % 1000);
  tuple_entry_1.set<int32_t>(value, context);
  tuple_entry_2.set<int32_t>(value + 1, context);
  tuple_entry_3.set_is_null(true, context);

  hashmap_entry_1.set<int32_t>(value, 0, context);
  hashmap_count_for_avg.set_is_null(true, 1, context);

  // Equal values
  EXPECT_TRUE(jit_aggregate_equals(tuple_entry_1, hashmap_entry_1, 0, context));

  // Unequal values
  EXPECT_FALSE(jit_aggregate_equals(tuple_entry_2, hashmap_entry_1, 0, context));

  // Comparing value to NULL
  EXPECT_FALSE(jit_aggregate_equals(tuple_entry_1, hashmap_count_for_avg, 1, context));
  EXPECT_FALSE(jit_aggregate_equals(tuple_entry_3, hashmap_entry_1, 0, context));

  // Comparing NULL to NULL
  EXPECT_TRUE(jit_aggregate_equals(tuple_entry_3, hashmap_count_for_avg, 1, context));
}

TEST_F(JitOperationsTest, JitAssign) {
  JitRuntimeContext context;
  context.tuple.resize(4);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(1);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value) {
    using ValueType = decltype(value);
    const JitTupleEntry tuple_entry{data_type, true, 0};
    const JitHashmapEntry hashmap_entry{data_type, true, 0};

    tuple_entry.set(value, context);
    jit_assign(tuple_entry, hashmap_entry, 0, context);

    EXPECT_FALSE(hashmap_entry.is_null(0, context));
    EXPECT_EQ(hashmap_entry.get<ValueType>(0, context), value);
  };

  typed_test(DataType::Int, static_cast<int32_t>(std::rand()));
  typed_test(DataType::Long, static_cast<int64_t>(std::rand()));
  typed_test(DataType::Float, static_cast<float>(std::rand()));
  typed_test(DataType::Double, static_cast<double>(std::rand()));
  typed_test(DataType::String, pmr_string("some string"));

  const JitTupleEntry tuple_entry{DataType::Int, true, 0};
  const JitHashmapEntry hashmap_entry{DataType::Int, true, 0};

  // Check jit_assign with NULL value
  tuple_entry.set_is_null(true, context);
  jit_assign(tuple_entry, hashmap_entry, 0, context);

  EXPECT_TRUE(hashmap_entry.is_null(0, context));
}

TEST_F(JitOperationsTest, JitGrowByOne) {
  JitRuntimeContext context;
  context.hashmap.columns.resize(5);

  // Perform the following test for each Hyrise data type
  const auto typed_test = [&](const DataType data_type, auto value, size_t column_index) {
    using ValueType = decltype(value);
    JitHashmapEntry hashmap_entry{data_type, false, column_index};

    // The vector should be empty initially
    EXPECT_EQ(context.hashmap.columns[0].get_vector<ValueType>().size(), 0u);

    // Add one of each possible initial values to the vector
    jit_grow_by_one(hashmap_entry, JitVariantVector::InitialValue::Zero, context);
    jit_grow_by_one(hashmap_entry, JitVariantVector::InitialValue::MaxValue, context);
    jit_grow_by_one(hashmap_entry, JitVariantVector::InitialValue::MinValue, context);

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
  typed_test(DataType::String, pmr_string{}, 4);
}

TEST_F(JitOperationsTest, JitAggregateCompute) {
  JitRuntimeContext context;
  context.tuple.resize(3);
  context.hashmap.columns.resize(1);
  context.hashmap.columns[0].resize(1);

  // jit_increment operation
  {
    const JitTupleEntry tuple_entry{DataType::String, false, 0};
    const JitTupleEntry null_tuple_entry{DataType::String, true, 1};
    const JitHashmapEntry hashmap_entry{DataType::Long, false, 0};

    tuple_entry.set<pmr_string>("some string", context);
    null_tuple_entry.set_is_null(true, context);
    hashmap_entry.set<int64_t>(5, 0, context);

    ASSERT_EQ(5, hashmap_entry.get<int64_t>(0, context));
    jit_aggregate_compute(jit_increment, tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(6, hashmap_entry.get<int64_t>(0, context));
    jit_aggregate_compute(jit_increment, tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(7, hashmap_entry.get<int64_t>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_increment, null_tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(7, hashmap_entry.get<int64_t>(0, context));
  }

  // jit_maximum operation
  {
    const JitTupleEntry tuple_entry_1{DataType::Float, false, 0};
    const JitTupleEntry tuple_entry_2{DataType::Float, false, 1};
    const JitTupleEntry null_tuple_entry{DataType::Float, true, 2};
    const JitHashmapEntry hashmap_entry{DataType::Float, false, 0};

    tuple_entry_1.set<float>(1.234f, context);
    tuple_entry_2.set<float>(3.456f, context);
    null_tuple_entry.set_is_null(true, context);
    hashmap_entry.set<float>(2.345f, 0, context);

    ASSERT_EQ(2.345f, hashmap_entry.get<float>(0, context));
    jit_aggregate_compute(jit_maximum, tuple_entry_1, hashmap_entry, 0, context);
    ASSERT_EQ(2.345f, hashmap_entry.get<float>(0, context));
    jit_aggregate_compute(jit_maximum, tuple_entry_2, hashmap_entry, 0, context);
    ASSERT_EQ(3.456f, hashmap_entry.get<float>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_maximum, null_tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(3.456f, hashmap_entry.get<float>(0, context));
  }

  // jit_minimum operation
  {
    const JitTupleEntry tuple_entry_1{DataType::Double, false, 0};
    const JitTupleEntry tuple_entry_2{DataType::Double, false, 1};
    const JitTupleEntry null_tuple_entry{DataType::Double, true, 2};
    const JitHashmapEntry hashmap_entry{DataType::Double, false, 0};

    tuple_entry_1.set<double>(3.456f, context);
    tuple_entry_2.set<double>(1.234f, context);
    null_tuple_entry.set_is_null(true, context);
    hashmap_entry.set<double>(2.345f, 0, context);

    ASSERT_EQ(2.345f, hashmap_entry.get<double>(0, context));
    jit_aggregate_compute(jit_minimum, tuple_entry_1, hashmap_entry, 0, context);
    ASSERT_EQ(2.345f, hashmap_entry.get<double>(0, context));
    jit_aggregate_compute(jit_minimum, tuple_entry_2, hashmap_entry, 0, context);
    ASSERT_EQ(1.234f, hashmap_entry.get<double>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_minimum, null_tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(1.234f, hashmap_entry.get<double>(0, context));
  }

  // jit_addition operation
  {
    const JitTupleEntry tuple_entry_1{DataType::Long, false, 0};
    const JitTupleEntry tuple_entry_2{DataType::Long, false, 1};
    const JitTupleEntry null_tuple_entry{DataType::Long, true, 2};
    const JitHashmapEntry hashmap_entry{DataType::Long, false, 0};

    tuple_entry_1.set<int64_t>(5, context);
    tuple_entry_2.set<int64_t>(-10, context);
    null_tuple_entry.set_is_null(true, context);
    hashmap_entry.set<int64_t>(100, 0, context);

    ASSERT_EQ(100, hashmap_entry.get<int64_t>(0, context));
    jit_aggregate_compute(jit_addition, tuple_entry_1, hashmap_entry, 0, context);
    ASSERT_EQ(105, hashmap_entry.get<int64_t>(0, context));
    jit_aggregate_compute(jit_addition, tuple_entry_2, hashmap_entry, 0, context);
    ASSERT_EQ(95, hashmap_entry.get<int64_t>(0, context));

    // NULL values should not change the aggregate
    jit_aggregate_compute(jit_addition, null_tuple_entry, hashmap_entry, 0, context);
    ASSERT_EQ(95, hashmap_entry.get<int64_t>(0, context));
  }
}

}  // namespace opossum
