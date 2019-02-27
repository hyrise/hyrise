#include "base_test.hpp"
#include "operators/jit_operator/operators/jit_compute.hpp"
#include "operators/jit_operator/operators/jit_expression.hpp"

namespace opossum {

class JitExpressionTest : public BaseTest {};

TEST_F(JitExpressionTest, Is_Not_Null) {
  JitRuntimeContext context;
  context.tuple.resize(2);

  auto tuple_entry = JitTupleEntry{DataType::Int, true, 0};
  auto result_index = 1;

  {
    JitExpression expression(std::make_shared<JitExpression>(tuple_entry), JitExpressionType::IsNull, result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Bool);
    ASSERT_FALSE(expression.result_entry().is_nullable());

    tuple_entry.set_is_null(true, context);
    expression.compute_and_store(context);
    ASSERT_TRUE(expression.result_entry().get<bool>(context));

    tuple_entry.set_is_null(false, context);
    expression.compute_and_store(context);
    ASSERT_FALSE(context.tuple.get<bool>(result_index));
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(tuple_entry), JitExpressionType::IsNotNull, result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Bool);
    ASSERT_FALSE(expression.result_entry().is_nullable());

    tuple_entry.set_is_null(true, context);
    expression.compute_and_store(context);
    ASSERT_FALSE(context.tuple.get<bool>(result_index));

    tuple_entry.set_is_null(false, context);
    expression.compute_and_store(context);
    ASSERT_TRUE(context.tuple.get<bool>(result_index));
  }
}

TEST_F(JitExpressionTest, Not) {
  JitRuntimeContext context;
  context.tuple.resize(2);
  auto result_index = 1;

  {
    auto tuple_entry = JitTupleEntry{DataType::Bool, false, 0};
    JitExpression expression(std::make_shared<JitExpression>(tuple_entry), JitExpressionType::Not, result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Bool);
    ASSERT_FALSE(expression.result_entry().is_nullable());

    tuple_entry.set<bool>(true, context);
    expression.compute_and_store(context);
    ASSERT_FALSE(expression.result_entry().get<bool>(context));

    tuple_entry.set<bool>(false, context);
    expression.compute_and_store(context);
    ASSERT_TRUE(context.tuple.get<bool>(result_index));
  }
  {
    auto tuple_entry = JitTupleEntry{DataType::Bool, true, 0};
    JitExpression expression(std::make_shared<JitExpression>(tuple_entry), JitExpressionType::Not, result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Bool);
    ASSERT_TRUE(expression.result_entry().is_nullable());

    tuple_entry.set_is_null(true, context);
    expression.compute_and_store(context);
    ASSERT_TRUE(context.tuple.is_null(result_index));

    tuple_entry.set_is_null(false, context);
    tuple_entry.set<bool>(true, context);
    expression.compute_and_store(context);
    ASSERT_FALSE(context.tuple.get<bool>(result_index));

    tuple_entry.set_is_null(false, context);
    tuple_entry.set<bool>(false, context);
    expression.compute_and_store(context);
    ASSERT_TRUE(context.tuple.get<bool>(result_index));
  }
  if (HYRISE_DEBUG) {
    // Not can only be computed on boolean values
    auto tuple_entry = JitTupleEntry{DataType::Long, false, 0};
    JitExpression expression(std::make_shared<JitExpression>(tuple_entry), JitExpressionType::Not, result_index);
    ASSERT_THROW(expression.compute_and_store(context), std::logic_error);
  }
}

TEST_F(JitExpressionTest, ArithmeticOperations) {
  JitRuntimeContext context;
  context.tuple.resize(6);
  auto result_index = 5;

  auto int_value = static_cast<int32_t>(std::rand());
  auto long_value = static_cast<int64_t>(std::rand());
  auto float_value = static_cast<float>(std::rand()) / RAND_MAX;
  auto double_value = static_cast<double>(std::rand()) / RAND_MAX;

  auto int_tuple_entry = JitTupleEntry{DataType::Int, false, 0};
  auto long_tuple_entry = JitTupleEntry{DataType::Long, false, 1};
  auto float_tuple_entry = JitTupleEntry{DataType::Float, false, 2};
  auto double_tuple_entry = JitTupleEntry{DataType::Double, false, 3};
  auto null_tuple_entry = JitTupleEntry{DataType::Int, true, 4};

  int_tuple_entry.set<int32_t>(int_value, context);
  long_tuple_entry.set<int64_t>(long_value, context);
  float_tuple_entry.set<float>(float_value, context);
  double_tuple_entry.set<double>(double_value, context);
  null_tuple_entry.set_is_null(true, context);

  {
    JitExpression expression(std::make_shared<JitExpression>(int_tuple_entry), JitExpressionType::Addition,
                             std::make_shared<JitExpression>(float_tuple_entry), result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Float);
    ASSERT_FALSE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_EQ(expression.result_entry().get<float>(context), int_value + float_value);
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(int_tuple_entry), JitExpressionType::Subtraction,
                             std::make_shared<JitExpression>(double_tuple_entry), result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Double);
    ASSERT_FALSE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_EQ(expression.result_entry().get<double>(context), int_value - double_value);
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(long_tuple_entry), JitExpressionType::Multiplication,
                             std::make_shared<JitExpression>(int_tuple_entry), result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Long);
    ASSERT_FALSE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_EQ(expression.result_entry().get<int64_t>(context), long_value * int_value);
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(float_tuple_entry), JitExpressionType::Division,
                             std::make_shared<JitExpression>(double_tuple_entry), result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Double);
    ASSERT_FALSE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_EQ(expression.result_entry().get<double>(context), float_value / double_value);
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(long_tuple_entry), JitExpressionType::Power,
                             std::make_shared<JitExpression>(double_tuple_entry), result_index);
    ASSERT_EQ(expression.result_entry().data_type(), DataType::Double);
    ASSERT_FALSE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_EQ(expression.result_entry().get<double>(context), std::pow(long_value, double_value));
  }

  // Check NULL semantics
  {
    JitExpression expression(std::make_shared<JitExpression>(null_tuple_entry), JitExpressionType::Addition,
                             std::make_shared<JitExpression>(null_tuple_entry), result_index);
    ASSERT_TRUE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_TRUE(expression.result_entry().is_null(context));
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(int_tuple_entry), JitExpressionType::Multiplication,
                             std::make_shared<JitExpression>(null_tuple_entry), result_index);
    ASSERT_TRUE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_TRUE(expression.result_entry().is_null(context));
  }
  {
    JitExpression expression(std::make_shared<JitExpression>(null_tuple_entry), JitExpressionType::Power,
                             std::make_shared<JitExpression>(int_tuple_entry), result_index);
    ASSERT_TRUE(expression.result_entry().is_nullable());
    expression.compute_and_store(context);
    ASSERT_TRUE(expression.result_entry().is_null(context));
  }
}

TEST_F(JitExpressionTest, PredicateOperations) {
  JitRuntimeContext context;
  context.tuple.resize(3);

  auto left_tuple_entry = JitTupleEntry{DataType::Int, false, 0};
  auto right_tuple_entry = JitTupleEntry{DataType::Int, false, 1};
  auto result_index = 2;

  JitExpression gt_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::GreaterThan,
                              std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression gte_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::GreaterThanEquals,
                               std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression lt_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::LessThan,
                              std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression lte_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::LessThanEquals,
                               std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression e_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::Equals,
                             std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression ne_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::NotEquals,
                              std::make_shared<JitExpression>(right_tuple_entry), result_index);

  ASSERT_EQ(gt_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(gte_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(lt_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(lte_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(e_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(ne_expression.result_entry().data_type(), DataType::Bool);

  for (auto i = 0; i < 10; ++i) {
    auto left_value = static_cast<int32_t>(std::rand()) % 5;
    auto right_value = static_cast<int32_t>(std::rand()) % 5;

    left_tuple_entry.set(left_value, context);
    right_tuple_entry.set(right_value, context);

    gt_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value > right_value);

    gte_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value >= right_value);

    lt_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value < right_value);

    lte_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value <= right_value);

    e_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value == right_value);

    ne_expression.compute_and_store(context);
    ASSERT_EQ(gt_expression.result_entry().get<bool>(context), left_value != right_value);
  }

  // Check that invalid data type combinations throw an exception
  auto string_tuple_entry = JitTupleEntry{DataType::String, false, 1};
  JitExpression invalid_expression(std::make_shared<JitExpression>(string_tuple_entry), JitExpressionType::Equals,
                                   std::make_shared<JitExpression>(right_tuple_entry), result_index);
  ASSERT_THROW(invalid_expression.compute_and_store(context), std::logic_error);
}

TEST_F(JitExpressionTest, StringComparison) {
  JitRuntimeContext context;
  context.tuple.resize(3);

  auto left_tuple_entry = JitTupleEntry{DataType::String, false, 0};
  auto right_tuple_entry = JitTupleEntry{DataType::String, false, 1};
  auto result_index = 2;

  JitExpression like_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::Like,
                                std::make_shared<JitExpression>(right_tuple_entry), result_index);
  JitExpression not_like_expression(std::make_shared<JitExpression>(left_tuple_entry), JitExpressionType::NotLike,
                                    std::make_shared<JitExpression>(right_tuple_entry), result_index);

  ASSERT_EQ(like_expression.result_entry().data_type(), DataType::Bool);
  ASSERT_EQ(not_like_expression.result_entry().data_type(), DataType::Bool);

  for (auto i = 0; i < 10; ++i) {
    auto left_value = pmr_string(1, 'a' + abs(static_cast<signed char>(std::rand())) % 5);
    auto right_value = pmr_string(1, 'a' + abs(static_cast<signed char>(std::rand())) % 5);

    left_tuple_entry.set(left_value, context);
    right_tuple_entry.set(right_value, context);

    like_expression.compute_and_store(context);
    auto a = like_expression.result_entry().get<bool>(context);
    auto b = left_value == right_value;
    ASSERT_EQ(a, b);

    not_like_expression.compute_and_store(context);
    ASSERT_EQ(not_like_expression.result_entry().get<bool>(context), left_value != right_value);
  }
}

TEST_F(JitExpressionTest, NestedExpressions) {
  JitRuntimeContext context;
  context.tuple.resize(10);

  auto a_tuple_entry = JitTupleEntry{DataType::Int, false, 0};
  auto b_tuple_entry = JitTupleEntry{DataType::Long, false, 1};
  auto c_tuple_entry = JitTupleEntry{DataType::Float, false, 2};
  auto d_tuple_entry = JitTupleEntry{DataType::Double, false, 3};

  // Compute "(A - (B * C)) / (D + B)"
  {
    auto b_times_c = std::make_shared<JitExpression>(std::make_shared<JitExpression>(b_tuple_entry),
                                                     JitExpressionType::Multiplication,
                                                     std::make_shared<JitExpression>(c_tuple_entry), 4);
    auto a_minus_b_times_c = std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry),
                                                             JitExpressionType::Subtraction, b_times_c, 5);
    auto d_plus_b =
        std::make_shared<JitExpression>(std::make_shared<JitExpression>(d_tuple_entry), JitExpressionType::Addition,
                                        std::make_shared<JitExpression>(b_tuple_entry), 6);
    JitExpression expression(a_minus_b_times_c, JitExpressionType::Division, d_plus_b, 7);

    for (auto i = 0; i < 10; ++i) {
      auto a_value = static_cast<int32_t>(std::rand());
      auto b_value = static_cast<int64_t>(std::rand());
      auto c_value = static_cast<float>(std::rand()) / RAND_MAX;
      auto d_value = static_cast<double>(std::rand()) / RAND_MAX;

      a_tuple_entry.set<int32_t>(a_value, context);
      b_tuple_entry.set<int64_t>(b_value, context);
      c_tuple_entry.set<float>(c_value, context);
      d_tuple_entry.set<double>(d_value, context);

      ASSERT_EQ(expression.result_entry().data_type(), DataType::Double);
      ASSERT_FALSE(expression.result_entry().is_nullable());
      expression.compute_and_store(context);
      ASSERT_EQ(expression.result_entry().get<double>(context), (a_value - (b_value * c_value)) / (d_value + b_value));
    }
  }

  // Compute "(A > B AND C >= D) OR (A + B) < C"
  {
    auto a_gt_b =
        std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry), JitExpressionType::GreaterThan,
                                        std::make_shared<JitExpression>(b_tuple_entry), 4);
    auto c_gte_d = std::make_shared<JitExpression>(std::make_shared<JitExpression>(c_tuple_entry),
                                                   JitExpressionType::GreaterThanEquals,
                                                   std::make_shared<JitExpression>(d_tuple_entry), 5);
    auto a_gt_b_and_c_gte_d = std::make_shared<JitExpression>(a_gt_b, JitExpressionType::And, c_gte_d, 6);

    auto a_plus_b =
        std::make_shared<JitExpression>(std::make_shared<JitExpression>(a_tuple_entry), JitExpressionType::Addition,
                                        std::make_shared<JitExpression>(b_tuple_entry), 7);
    auto a_plus_b_lt_c = std::make_shared<JitExpression>(a_plus_b, JitExpressionType::LessThan,
                                                         std::make_shared<JitExpression>(c_tuple_entry), 8);
    JitExpression expression(a_gt_b_and_c_gte_d, JitExpressionType::Or, a_plus_b_lt_c, 9);

    for (auto i = 0; i < 10; ++i) {
      auto a_value = static_cast<int32_t>(std::rand());
      auto b_value = static_cast<int64_t>(std::rand());
      auto c_value = static_cast<float>(std::rand()) / RAND_MAX;
      auto d_value = static_cast<double>(std::rand()) / RAND_MAX;

      a_tuple_entry.set<int32_t>(a_value, context);
      b_tuple_entry.set<int64_t>(b_value, context);
      c_tuple_entry.set<float>(c_value, context);
      d_tuple_entry.set<double>(d_value, context);

      ASSERT_EQ(expression.result_entry().data_type(), DataType::Bool);
      ASSERT_FALSE(expression.result_entry().is_nullable());
      expression.compute_and_store(context);
      ASSERT_EQ(expression.result_entry().get<bool>(context),
                (a_value > b_value && c_value >= d_value) || a_value + b_value < c_value);
    }
  }
}

}  // namespace opossum
