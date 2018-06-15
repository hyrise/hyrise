#include "gtest/gtest.h"

#include <optional>

#include "expression/list_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/extract_expression.hpp"
#include "expression/exists_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "expression/function_expression.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/aggregate.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "utils/load_table.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    table_a = load_table("src/test/tables/expression_evaluator/input_a.tbl");
    chunk_a = table_a->get_chunk(ChunkID{0});

    a = PQPColumnExpression::from_table(*table_a, "a");
    b = PQPColumnExpression::from_table(*table_a, "b");
    c = PQPColumnExpression::from_table(*table_a, "c");
    d = PQPColumnExpression::from_table(*table_a, "d");
    e = PQPColumnExpression::from_table(*table_a, "e");
    f = PQPColumnExpression::from_table(*table_a, "f");
    s1 = PQPColumnExpression::from_table(*table_a, "s1");
    s2 = PQPColumnExpression::from_table(*table_a, "s2");
    s3 = PQPColumnExpression::from_table(*table_a, "s3");
    dates = PQPColumnExpression::from_table(*table_a, "dates");
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);
    a_plus_c = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, c);
    s1_gt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, s1, s2);
    s1_lt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, s1, s2);
    a_lt_b = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, b);
    a_lt_c = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, c);

    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl");
    x = PQPColumnExpression::from_table(*table_b, "x");

    table_bools = load_table("src/test/tables/expression_evaluator/input_bools.tbl");
    chunk_bools = table_bools->get_chunk(ChunkID{0});
    bool_a = PQPColumnExpression::from_table(*table_bools, "a");
    bool_b = PQPColumnExpression::from_table(*table_bools, "b");
    bool_c = PQPColumnExpression::from_table(*table_bools, "c");
  }

  /**
   * Turn an ExpressionResult<T> into a canonical form "std::vector<std::optional<T>>" to make the writing of tests
   * easier.
   */
  template<typename T>
  std::vector<std::optional<T>> normalize_expression_result(const ExpressionResult<T> &result) {
    std::vector<std::optional<T>> normalized(result.size());

    result.as_view([&](const auto &resolved) {
      for (auto idx = size_t{0}; idx < result.size(); ++idx) {
        if (!resolved.null(idx)) normalized[idx] = resolved.value(idx);
      }
    });

    return normalized;
  }

  template<typename R>
  void print(const std::vector<std::optional<R>>& values_or_nulls) {
    for (const auto& value_or_null : values_or_nulls) {
      if (value_or_null) {
        std::cout << *value_or_null << ", ";
      } else {
        std::cout << "NULL, ";
      }
    }
  }

  template<typename R>
  bool test_expression(const std::shared_ptr<Table>& table,
                       const AbstractExpression& expression,
                       const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator{table, ChunkID{0}}.evaluate_expression_to_result<R>(expression);
    const auto actual_normalized = normalize_expression_result(*actual_result);
    if (actual_normalized == expected) return true;

    std::cout << "Actual:\n  ";
    print(actual_normalized);
    std::cout << std::endl;
    std::cout << "Expected:\n  ";
    print(expected);
    std::cout << std::endl;

    return false;
  }

  template<typename R>
  bool test_expression(const AbstractExpression& expression,
                       const std::vector<std::optional<R>>& expected) {
    const auto actual_result = ExpressionEvaluator{}.evaluate_expression_to_result<R>(expression);
    const auto actual_normalized = normalize_expression_result(*actual_result);
    if (actual_normalized == expected) return true;

    std::cout << "Actual:\n  ";
    print(actual_normalized);
    std::cout << std::endl;
    std::cout << "Expected:\n  ";
    print(expected);
    std::cout << std::endl;

    return false;
  }

  std::shared_ptr<Table> table_a, table_b, table_bools;
  std::shared_ptr<Chunk> chunk_a, chunk_bools;

  std::shared_ptr<PQPColumnExpression> a, b, c, d, e, f, s1, s2, s3, dates, x, bool_a, bool_b, bool_c;
  std::shared_ptr<ArithmeticExpression> a_plus_b;
  std::shared_ptr<ArithmeticExpression> a_plus_c;
  std::shared_ptr<BinaryPredicateExpression> a_lt_b;
  std::shared_ptr<BinaryPredicateExpression> a_lt_c;
  std::shared_ptr<BinaryPredicateExpression> s1_gt_s2;
  std::shared_ptr<BinaryPredicateExpression> s1_lt_s2;
};

TEST_F(ExpressionEvaluatorTest, TernaryOrLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*or_(1, 0), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(1, 1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(0, 1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(0, 0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(0, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(1, NullValue{}), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(NullValue{}, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(NullValue{}, 0), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*or_(NullValue{}, 1), {1}));
}

TEST_F(ExpressionEvaluatorTest, TernaryOrSeries) {
  // clang-format off
  EXPECT_TRUE(test_expression<int32_t>(table_bools, *or_(bool_a, bool_b), {0, 0, 0, 1, 1, 1, 1, 1, 1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_bools, *or_(bool_a, bool_c), {0, 1, std::nullopt, 0, 1, std::nullopt, 1, 1, 1, 1, 1, 1}));  // NOLINT
  // clang-format on
}

TEST_F(ExpressionEvaluatorTest, TernaryAndLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*and_(1, 0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(1, 1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(0, 1), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(0, 0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(0, NullValue{}), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(1, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(NullValue{}, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(NullValue{}, 0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*and_(NullValue{}, 1), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ArithmeticsLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*mul(5, 3), {15}));
  EXPECT_TRUE(test_expression<int32_t>(*mul(5, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*sub(15, 12), {3}));
  EXPECT_TRUE(test_expression<float>(*division(10.0, 4.0), {2.5f}));
  EXPECT_TRUE(test_expression<int32_t>(*sub(NullValue{}, NullValue{}), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ArithmeticsSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *mul(a, b), {2, 6, 12, 20}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add(a, add(b, c)), {36, std::nullopt, 41, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add(a, NullValue{}), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add(a, add(b, NullValue{})), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, CaseLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, 2, 1), {2}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2, 1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2, case_(1, 5, 13)), {5}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(NullValue{}, 42, add(5, 3)), {8}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, NullValue{}, 5), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, CaseSeries) {
  // clang-format off
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(greater_than(c, a), b, 1337), {2, 1337, 4, 1337}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(greater_than(c, 0), NullValue{}, c), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(1, c, a), {33, std::nullopt, 34, std::nullopt}));  // NOLINT
  // clang-format on
}

TEST_F(ExpressionEvaluatorTest, IsNullLiteral) {
  EXPECT_TRUE(test_expression<int32_t>(*is_null(0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*is_null(1), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*is_null(null()), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null(0), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null(1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null(null()), {0}));
}

TEST_F(ExpressionEvaluatorTest, IsNullSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *is_null(add(c, a)), {0, 1, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *is_not_null(add(c, a)), {1, 0, 1, 0}));
}

TEST_F(ExpressionEvaluatorTest, SubstrLiterals) {
  /** Hyrise follows SQLite semantics for negative indices in SUBSTR */

  EXPECT_TRUE(test_expression<std::string>(*substr("", 3, 4), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", 4, 4), {"lo W"}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -18, 4), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -12, 1), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -12, 2), {"H"}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -12, 12), {"Hello World"}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -5, 2), {"Wo"}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", -5, -2), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", 4, 40), {"lo World"}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", 20, 1), {""}));
  // TODO(moritz) enable once casting expressions are in, so SUBSTR can cast this 4ul -> 4i
  //EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", int64_t{4}, 4), {"lo W"}));
  EXPECT_TRUE(test_expression<std::string>(*substr(null(), 1, 2), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", null(), 2), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", 2, null()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, SubstrSeries) {
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr(s1, 2, 3), {"", "ell", "hat", "ame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr(s3, 4, 1), {std::nullopt, "d", "l", std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr(s1, a, b), {"a", "ell", "at", "e"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr(s3, 2, a), {std::nullopt, "bc", "yzl", std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr("test", 2, c), {"est", std::nullopt, "est", std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ConcatLiterals) {
  EXPECT_TRUE(test_expression<std::string>(*concat(null(), "world"), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*concat("hello ", "world"), {"hello world"}));
  EXPECT_TRUE(test_expression<std::string>(*concat("hello", " ", "world"), {"hello world"}));
  EXPECT_TRUE(test_expression<std::string>(*concat("hello", " ", "world", " are you, ", "okay?"), {"hello world are you, okay?"}));
  EXPECT_TRUE(test_expression<std::string>(*concat("hello", " ", null(), " are you, ", "okay?"), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ConcatSeries) {
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat(s1, s2), {"ab", "HelloWorld", "whatup", "SameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat("yo", s1, s2), {"yoab", "yoHelloWorld", "yowhatup", "yoSameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat(concat("a", "b", "c"), s1, s2), {"abcab", "abcHelloWorld", "abcwhatup", "abcSameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat("nope", s1, null()), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat(s1, s2, s3), {std::nullopt, "HelloWorldabcd", "whatupxyzlol", std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, Parameter) {
  const auto a_id = ParameterID{0};
  const auto b_id = ParameterID{1};

  auto a_plus_5_times_b = mul(add(parameter(a_id, a), 5), parameter(b_id, b));

  expression_set_parameters(a_plus_5_times_b, {{a_id, 12}, {b_id, 2}});
  EXPECT_TRUE(test_expression<int32_t>(*a_plus_5_times_b, {34}));

  expression_set_parameters(a_plus_5_times_b, {{b_id, 4}});
  EXPECT_TRUE(test_expression<int32_t>(*a_plus_5_times_b, {68}));

}

//TEST_F(ExpressionEvaluatorTest, ArithmeticExpressionWithNull) {
//  const auto actual_result = boost::get<NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_plus_c));
//  const auto& actual_values = actual_result.first;
//  const auto& actual_nulls = actual_result.second;
//
//  ASSERT_EQ(actual_values.size(), 4u);
//  EXPECT_EQ(actual_values.at(0), 34);
//  EXPECT_EQ(actual_values.at(2), 37);
//
//  std::vector<bool> expected_nulls = {false, true, false, true};
//  EXPECT_EQ(actual_nulls, expected_nulls);
//}
//
//TEST_F(ExpressionEvaluatorTest, GreaterThanWithStrings) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*s1_gt_s2));
//
//  std::vector<int32_t> expected_values = {0, 0, 1, 0};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThanWithStrings) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*s1_lt_s2));
//
//  std::vector<int32_t> expected_values = {1, 1, 0, 0};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThan) {
//  const auto actual_values = boost::get<NonNullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_lt_b));
//
//  std::vector<int32_t> expected_values = {1, 1, 1, 1};
//  EXPECT_EQ(actual_values, expected_values);
//}
//
//TEST_F(ExpressionEvaluatorTest, LessThanWithNulls) {
//  const auto actual_result = boost::get<NullableValues<int32_t>>(evaluator->evaluate_expression<int32_t>(*a_lt_c));
//  const auto& actual_values = actual_result.first;
//  const auto& actual_nulls = actual_result.second;
//
//  ASSERT_EQ(actual_values.size(), 4u);
//  EXPECT_TRUE(actual_values.at(0));
//  EXPECT_TRUE(actual_values.at(2));
//
//  std::vector<bool> expected_nulls = {false, true, false, true};
//  EXPECT_EQ(actual_nulls, expected_nulls);
//}

TEST_F(ExpressionEvaluatorTest, InListLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*in(null(), list()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in(null(), list(1, 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in(null(), list(null(), 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in(5, list()), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in(5, list(null(), 5, null())), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*in(5, list(null(), 6, null())), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in(5, list(1.0, 3.0)), {0}));
}

TEST_F(ExpressionEvaluatorTest, InListSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(a, list(1.0, 3.0)),  {1, 0, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(a, list(null(), 1.0, 3.0)),  {1, std::nullopt, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(sub(mul(a, 2), 2), list(b, 6, null(), 0)),  {1, std::nullopt, 1, 1}));
}

TEST_F(ExpressionEvaluatorTest, InSelectUncorelated) {
  // PQP that returns the column "a"
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto pqp_a = std::make_shared<Projection>(table_wrapper_a, expression_vector(PQPColumnExpression::from_table(*table_a, "a")));
  const auto select_a = select(pqp_a, DataType::Int, false);

  // PQP that returns the column "c"
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto pqp_b = std::make_shared<Projection>(table_wrapper_b, expression_vector(PQPColumnExpression::from_table(*table_a, "c")));
  const auto select_b = select(pqp_b, DataType::Int, true);

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(6, select_a),  {0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(a, select_a),  {1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(add(a, 2), select_a),  {1, 1, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(b, select_a),  {1, 1, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(34, select_b),  {1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(34.0, select_b),  {1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(34.5, select_b),  {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in("hello", select_b),  {0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(c, select_b),  {1, std::nullopt, 1, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, InSelectCorelated) {
  // PQP that returns the column "b" multiplied with the current value in "a"
  //
  // row   list returned from select
  //  0      (1, 2, 3, 4)
  //  1      (2, 4, 6, 8)
  //  2      (3, 6, 9, 12)
  //  3      (4, 8, 12, 16)
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto mul_a = mul(parameter(ParameterID{0}), PQPColumnExpression::from_table(*table_a, "a"));
  const auto pqp_a = std::make_shared<Projection>(table_wrapper_a, expression_vector(mul_a));
  const auto select_a = select(pqp_a, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(4, select_a),  {1, 1, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(6, select_a),  {0, 1, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(16, select_a),  {0, 0, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(b, select_a),  {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(null(), select_a),  {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));

  // PQP that returns the column "c" added to the current value in "a"
  //
  // row   list returned from select
  //  0      (34, NULL, 35, NULL)
  //  1      (35, NULL, 36, NULL)
  //  2      (36, NULL, 37, NULL)
  //  3      (37, NULL, 38, NULL)
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto add_b = add(parameter(ParameterID{0}), PQPColumnExpression::from_table(*table_a, "c"));
  const auto pqp_b = std::make_shared<Projection>(table_wrapper_b, expression_vector(add_b));
  const auto select_b = select(pqp_b, DataType::Int, true, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(34, select_b),  {1, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(35, select_b),  {1, 1, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(null(), select_b),  {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(36, select_b),  {std::nullopt, 1, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(36.0, select_b),  {std::nullopt, 1, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in(36.3, select_b),  {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, Exists) {
  /**
   * Test a co-related EXISTS query
   *
   * SELECT
   *    EXISTS (SELECT a + x, x FROM table_b WHERE a + x = 13)
   * FROM
   *    table_a;
   */
  const auto table_wrapper = std::make_shared<TableWrapper>(table_b);
  const auto parameter_a = parameter(ParameterID{0});
  const auto a_plus_x_projection = std::make_shared<Projection>(table_wrapper, expression_vector(add(parameter_a, x), x));
  const auto a_plus_x_eq_13_scan = std::make_shared<TableScan>(table_wrapper, ColumnID{0}, PredicateCondition::Equals, 13);
  const auto pqp_select_expression = select(a_plus_x_eq_13_scan, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  const auto exists_expression = std::make_shared<ExistsExpression>(pqp_select_expression);
  EXPECT_TRUE(test_expression<int32_t>(table_a, *exists_expression,  {0, 0, 1, 1}));
}

//TEST_F(ExpressionEvaluatorTest, Extract) {
//  const auto extract_year_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Year, dates);
//  const auto actual_years = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_year_expression));
//  const auto expected_years = std::vector<std::string>({"2017", "2014", "2011", "2010"});
//  EXPECT_EQ(actual_years, expected_years);
//
//  const auto extract_month_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Month, dates);
//  const auto actual_months = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_month_expression));
//  const auto expected_months = std::vector<std::string>({"12", "08", "09", "01"});
//  EXPECT_EQ(actual_months, expected_months);
//
//  const auto extract_day_expression = std::make_shared<ExtractExpression>(DatetimeComponent::Day, dates);
//  const auto actual_days = boost::get<NonNullableValues<std::string>>(evaluator->evaluate_expression<std::string>(*extract_day_expression));
//  const auto expected_days = std::vector<std::string>({"06", "05", "03", "02"});
//  EXPECT_EQ(actual_days, expected_days);
//}
//TEST_F(ExpressionEvaluatorTest, PQPSelectExpression) {
//  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_b);
//  const auto external_b = std::make_shared<ValuePlaceholderExpression>(ValuePlaceholder{0});
//  const auto b_plus_x = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, external_b, x);
//  const auto inner_expressions = std::vector<std::shared_ptr<AbstractExpression>>({b_plus_x, x});
//  const auto inner_projection = std::make_shared<Projection>(table_wrapper_b, inner_expressions);
//  const auto table_scan = std::make_shared<TableScan>(inner_projection, ColumnID{0}, PredicateCondition::Equals, 12);
//  const auto aggregates = std::vector<AggregateColumnDefinition>({{AggregateFunction::Sum, ColumnID{1}, "SUM(b)"}});
//  const auto aggregate = std::make_shared<Aggregate>(table_scan, aggregates, std::vector<ColumnID>{});
//
//  const auto parameters = std::vector<ColumnID>({ColumnID{1}});
//  const auto pqp_select_expression = std::make_shared<PQPSelectExpression>(aggregate, DataType::Int, true, parameters);
//
//  const auto expected_result = std::vector<int64_t>({20, 9, 24, 7});
//  EXPECT_EQ(boost::get<std::vector<int64_t>>(evaluator->evaluate_expression<int64_t>(*pqp_select_expression)), expected_result);
//}

}  // namespace opossum