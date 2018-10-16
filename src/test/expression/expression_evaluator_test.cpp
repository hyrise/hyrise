#include <optional>

#include "gtest/gtest.h"

#include "expression/arithmetic_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/value_expression.hpp"
#include "operators/aggregate.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"
#include "operators/table_scan.hpp"
#include "operators/table_wrapper.hpp"
#include "storage/storage_manager.hpp"
#include "storage/table.hpp"
#include "testing_assert.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionEvaluatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    // Load table_a
    table_a = load_table("src/test/tables/expression_evaluator/input_a.tbl");
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
    dates2 = PQPColumnExpression::from_table(*table_a, "dates2");
    a_plus_b = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, b);
    a_plus_c = std::make_shared<ArithmeticExpression>(ArithmeticOperator::Addition, a, c);
    s1_gt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::GreaterThan, s1, s2);
    s1_lt_s2 = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, s1, s2);
    a_lt_b = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, b);
    a_lt_c = std::make_shared<BinaryPredicateExpression>(PredicateCondition::LessThan, a, c);

    // Load table_b
    table_b = load_table("src/test/tables/expression_evaluator/input_b.tbl");
    x = PQPColumnExpression::from_table(*table_b, "x");

    // Load table_bools
    table_bools = load_table("src/test/tables/expression_evaluator/input_bools.tbl");
    bool_a = PQPColumnExpression::from_table(*table_bools, "a");
    bool_b = PQPColumnExpression::from_table(*table_bools, "b");
    bool_c = PQPColumnExpression::from_table(*table_bools, "c");

    // Create table_empty
    TableColumnDefinitions empty_table_columns;
    empty_table_columns.emplace_back("a", DataType::Int, false);
    empty_table_columns.emplace_back("b", DataType::Float, true);
    empty_table_columns.emplace_back("s", DataType::String, false);
    table_empty = std::make_shared<Table>(empty_table_columns, TableType::Data);

    Segments segments;
    segments.emplace_back(std::make_shared<ValueSegment<int32_t>>(pmr_concurrent_vector<int32_t>{}));
    segments.emplace_back(
        std::make_shared<ValueSegment<float>>(pmr_concurrent_vector<float>{}, pmr_concurrent_vector<bool>{}));
    segments.emplace_back(std::make_shared<ValueSegment<std::string>>(pmr_concurrent_vector<std::string>{}));
    table_empty->append_chunk(segments);

    empty_a = PQPColumnExpression::from_table(*table_empty, "a");
    empty_b = PQPColumnExpression::from_table(*table_empty, "b");
    empty_s = PQPColumnExpression::from_table(*table_empty, "s");
  }

  /**
   * Turn an ExpressionResult<T> into a canonical form "std::vector<std::optional<T>>" to make the writing of tests
   * easier.
   */
  template <typename T>
  std::vector<std::optional<T>> normalize_expression_result(const ExpressionResult<T>& result) {
    std::vector<std::optional<T>> normalized(result.size());

    result.as_view([&](const auto& resolved) {
      for (auto idx = size_t{0}; idx < result.size(); ++idx) {
        if (!resolved.is_null(idx)) normalized[idx] = resolved.value(idx);
      }
    });

    return normalized;
  }

  template <typename R>
  void print(const std::vector<std::optional<R>>& values_or_nulls) {
    for (const auto& value_or_null : values_or_nulls) {
      if (value_or_null) {
        std::cout << *value_or_null << ", ";
      } else {
        std::cout << "NULL, ";
      }
    }
  }

  template <typename R>
  bool test_expression(const std::shared_ptr<Table>& table, const AbstractExpression& expression,
                       const std::vector<std::optional<R>>& expected,
                       const std::shared_ptr<const ExpressionEvaluator::UncorrelatedSelectResults>&
                           uncorrelated_select_results = nullptr) {
    const auto actual_result =
        ExpressionEvaluator{table, ChunkID{0}, uncorrelated_select_results}.evaluate_expression_to_result<R>(
            expression);
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

  template <typename R>
  bool test_expression(const AbstractExpression& expression, const std::vector<std::optional<R>>& expected) {
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

  std::shared_ptr<Table> table_empty, table_a, table_b, table_bools;

  std::shared_ptr<PQPColumnExpression> a, b, c, d, e, f, s1, s2, s3, dates, dates2, x, bool_a, bool_b, bool_c;
  std::shared_ptr<PQPColumnExpression> empty_a, empty_b, empty_s;
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
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *or_(less_than_(1, empty_a), less_than_(1, empty_a)), {}));
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

TEST_F(ExpressionEvaluatorTest, TernaryAndSeries) {
  // clang-format off
  EXPECT_TRUE(test_expression<int32_t>(table_bools, *and_(bool_a, bool_b), {0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_bools, *and_(bool_a, bool_c), {0, 0, 0, 0, 0, 0, 0, 1, std::nullopt, 0, 1, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *and_(less_than_(1, empty_a), less_than_(1, empty_a)), {}));
  // clang-format on
}

TEST_F(ExpressionEvaluatorTest, ValueLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*value_(5), {5}));
  EXPECT_TRUE(test_expression<float>(*value_(5.0f), {5.0f}));
  EXPECT_TRUE(test_expression<int32_t>(*value_(NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<float>(*value_(NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*value_("Hello"), {"Hello"}));
  EXPECT_TRUE(test_expression<std::string>(*value_(NullValue{}), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ArithmeticsLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*mul_(5, 3), {15}));
  EXPECT_TRUE(test_expression<int32_t>(*mul_(5, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*add_(5, 6), {11}));
  EXPECT_TRUE(test_expression<float>(*add_(5, 6), {11.0}));
  EXPECT_TRUE(test_expression<int32_t>(*sub_(15, 12), {3}));
  EXPECT_TRUE(test_expression<float>(*div_(10.0, 4.0), {2.5f}));
  EXPECT_TRUE(test_expression<float>(*div_(10.0, 0), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*div_(10, 0), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*sub_(NullValue{}, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*mod_(5, 3), {2}));
  EXPECT_TRUE(test_expression<float>(*mod_(23.25, 3), {2.25}));
  EXPECT_TRUE(test_expression<float>(*mod_(23.25, 0), {std::nullopt}));
  EXPECT_TRUE(test_expression<float>(*mod_(5, 0), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ArithmeticsSeries) {
  // clang-format off
  EXPECT_TRUE(test_expression<int32_t>(table_a, *mul_(a, b), {2, 6, 12, 20}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *mod_(b, a), {0, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *mod_(a, c), {1, std::nullopt, 3, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add_(a, add_(b, c)), {36, std::nullopt, 41, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add_(a, NullValue{}), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_a, *add_(a, add_(b, NullValue{})), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *add_(empty_a, empty_b), {}));
  // clang-format on
}

TEST_F(ExpressionEvaluatorTest, PredicatesLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(5, 3.3), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(5, 5.0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(5.1, 5.0), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(null_(), 5.0), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(5.0, null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_(null_(), null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_("Hello", "Wello"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_("Wello", "Hello"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_("Wello", null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_equals_(5.3, 3), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_equals_(5.3, 5.3), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_equals_(5.3, 5.4f), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*greater_than_equals_(5.5f, 5.4), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_(5.2f, 5.4), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_(5.5f, 5.4), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_(5.4, 5.4), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_equals_(5.3, 5.4), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_equals_(5.4, 5.4), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*less_than_equals_(5.5, 5.4), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*equals_(5.5f, 5.5f), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*equals_(5.5f, 5.7f), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*equals_("Hello", "Hello"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*equals_("Hello", "hello"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*equals_("Hello", null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*not_equals_(5.5f, 5), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*not_equals_(5.5f, 5.5f), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(3, 3.0, 5.0), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(3, 3.1, 5.0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(5.0f, 3.1, 5), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(5.1f, 3.1, 5), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(5.1f, 3.1, null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(5.1f, null_(), 5), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(null_(), 3.1, 5), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*between_(null_(), null_(), null_()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, PredicatesSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *greater_than_(b, a), {1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *greater_than_(s1, s2), {0, 0, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *greater_than_(b, null_()),
                                       {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *greater_than_(c, a), {1, std::nullopt, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *greater_than_equals_(b, mul_(a, 2)), {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *equals_(b, mul_(a, 2)), {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *not_equals_(b, mul_(a, 2)), {0, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *less_than_(b, mul_(a, 2)), {0, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *less_than_equals_(b, mul_(a, 2)), {1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *less_than_equals_(c, f), {1, std::nullopt, 0, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *between_(b, a, c), {1, std::nullopt, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *between_(e, a, f), {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *between_(3.3, a, b), {0, 0, 1, 0}));
}

TEST_F(ExpressionEvaluatorTest, CaseLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, 2, 1), {2}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, NullValue{}, 1), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, 2.3, 1), {2.3}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2.3, 1), {1.0}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2.3, NullValue{}), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2, 1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(0, 2, case_(1, 5, 13)), {5}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(NullValue{}, 42, add_(5, 3)), {8}));
  EXPECT_TRUE(test_expression<int32_t>(*case_(1, NullValue{}, 5), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, CaseSeries) {
  // clang-format off
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(greater_than_(c, a), b, 1337), {2, 1337, 4, 1337}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(greater_than_(c, 0), NullValue{}, c), {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_a, *case_(1, c, a), {33, std::nullopt, 34, std::nullopt}));  // NOLINT
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *case_(greater_than_(empty_a, 3), 1, 2), {}));
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *case_(1, empty_a, empty_a), {}));
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *case_(greater_than_(empty_a, 3), empty_a, empty_a), {}));
  // clang-format on
}

TEST_F(ExpressionEvaluatorTest, IsNullLiteral) {
  EXPECT_TRUE(test_expression<int32_t>(*is_null_(0), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*is_null_(1), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*is_null_(null_()), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null_(0), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null_(1), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*is_not_null_(null_()), {0}));
}

TEST_F(ExpressionEvaluatorTest, IsNullSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *is_null_(add_(c, a)), {0, 1, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *is_not_null_(add_(c, a)), {1, 0, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *is_not_null_(empty_a), {}));
}

TEST_F(ExpressionEvaluatorTest, NegateLiteral) {
  EXPECT_TRUE(test_expression<double>(*unary_minus_(2.5), {-2.5}));
  EXPECT_TRUE(test_expression<int32_t>(*unary_minus_(int32_t{-3}), {int32_t{3}}));
}

TEST_F(ExpressionEvaluatorTest, NegateSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *unary_minus_(a), {-1, -2, -3, -4}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *unary_minus_(c), {-33, std::nullopt, -34, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *unary_minus_(unary_minus_(c)), {33, std::nullopt, 34, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, LikeLiteral) {
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", "hello"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", "Hello"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_("hello", "Hello"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", "h_ll%o"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_("hello", "h_ll%o"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", "H_ll_o"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_("hello", "H_ll_o"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", "%h%_l%o"), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_("hello", "%h%_l%o"), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*like_(null_(), "%h%_l%o"), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_(null_(), "%h%_l%o"), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*like_(null_(), null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_(null_(), null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*like_("hello", null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*not_like_("hello", null_()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, LikeSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s1, concat_(s1, "%")), {1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s1, concat_(s1, "a")), {0, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *not_like_(s1, "%a%"), {0, 1, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s1, "%A%"), {0, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s1, "%H%e%_%l%"), {0, 1, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *not_like_(s1, "%H%e%_%l%"), {1, 0, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s3, "%a%"), {std::nullopt, 1, 0, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *not_like_(s3, "%a%"), {std::nullopt, 0, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_(s1, "%a%"), {1, 0, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *like_("Same", s1), {0, 0, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *not_like_("Same", s1), {1, 1, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *like_(empty_s, "hello"), {}));
  EXPECT_TRUE(test_expression<int32_t>(table_empty, *like_("hello", empty_s), {}));
}

TEST_F(ExpressionEvaluatorTest, SubstrLiterals) {
  /** Hyrise follows SQLite semantics for negative indices in SUBSTR */

  EXPECT_TRUE(test_expression<std::string>(*substr_("", 3, 4), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", 4, 4), {"lo W"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -18, 4), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -12, 1), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -12, 2), {"H"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -12, 12), {"Hello World"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -5, 2), {"Wo"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", -5, -2), {""}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", 4, 40), {"lo World"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", 20, 1), {""}));
  // TODO(moritz) enable once casting expressions are in, so SUBSTR can cast this 4ul -> 4i
  //  EXPECT_TRUE(test_expression<std::string>(*substr("Hello World", int64_t{4}, 4), {"lo W"}));
  EXPECT_TRUE(test_expression<std::string>(*substr_(null_(), 1, 2), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", null_(), 2), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*substr_("Hello World", 2, null_()), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, SubstrSeries) {
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr_(s1, 2, 3), {"", "ell", "hat", "ame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr_(s3, 4, 1), {std::nullopt, "d", "l", std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr_(s1, a, b), {"a", "ell", "at", "e"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *substr_(s3, 2, a), {std::nullopt, "bc", "yzl", std::nullopt}));
  EXPECT_TRUE(
      test_expression<std::string>(table_a, *substr_("test", 2, c), {"est", std::nullopt, "est", std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_empty, *substr_(empty_s, 1, empty_a), {}));
}

TEST_F(ExpressionEvaluatorTest, ConcatLiterals) {
  EXPECT_TRUE(test_expression<std::string>(*concat_(null_(), "world"), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*concat_("hello ", "world"), {"hello world"}));
  EXPECT_TRUE(test_expression<std::string>(*concat_("hello", " ", "world"), {"hello world"}));
  EXPECT_TRUE(test_expression<std::string>(*concat_("hello", " ", "world", " are you, ", "okay?"),
                                           {"hello world are you, okay?"}));
  EXPECT_TRUE(test_expression<std::string>(*concat_("hello", " ", null_(), " are you, ", "okay?"), {std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, ConcatSeries) {
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat_(s1, s2), {"ab", "HelloWorld", "whatup", "SameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat_("yo", s1, s2),
                                           {"yoab", "yoHelloWorld", "yowhatup", "yoSameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat_(concat_("a", "b", "c"), s1, s2),
                                           {"abcab", "abcHelloWorld", "abcwhatup", "abcSameSame"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat_("nope", s1, null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *concat_(s1, s2, s3),
                                           {std::nullopt, "HelloWorldabcd", "whatupxyzlol", std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_empty, *concat_(empty_s, "hello"), {}));
}

TEST_F(ExpressionEvaluatorTest, Parameter) {
  const auto a_id = ParameterID{0};
  const auto b_id = ParameterID{1};

  auto a_plus_5_times_b = mul_(add_(correlated_parameter_(a_id, a), 5), correlated_parameter_(b_id, b));

  expression_set_parameters(a_plus_5_times_b, {{a_id, 12}, {b_id, 2}});
  EXPECT_TRUE(test_expression<int32_t>(*a_plus_5_times_b, {34}));

  expression_set_parameters(a_plus_5_times_b, {{b_id, 4}});
  EXPECT_TRUE(test_expression<int32_t>(*a_plus_5_times_b, {68}));
}

TEST_F(ExpressionEvaluatorTest, InListLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*in_(null_(), list_(null_())), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(null_(), list_(null_(), 3)), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(null_(), list_()), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(null_(), list_(1, 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(null_(), list_(null_(), 2, 3, 4)), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(null_(), 5, null_())), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_()), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(null_(), add_(2, 3), null_())), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(null_(), 6, null_())), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(1, 3)), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(1.0, 3.0)), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(1.0, 5.0)), {1}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(1.0, add_(1.0, 3.0))), {0}));
  EXPECT_TRUE(test_expression<int32_t>(*in_(5, list_(1.0, add_(2.0, 3.0))), {1}));
}

TEST_F(ExpressionEvaluatorTest, InListSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, list_(1.0, 3.0)), {1, 0, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, list_(null_(), 1.0, 3.0)), {1, std::nullopt, 1, std::nullopt}));
  EXPECT_TRUE(
      test_expression<int32_t>(table_a, *in_(sub_(mul_(a, 2), 2), list_(b, 6, null_(), 0)), {1, std::nullopt, 1, 1}));
}

TEST_F(ExpressionEvaluatorTest, InArbitraryExpression) {
  // We support `<expression_a> IN <expression_b>`, even though it looks weird, because <expression_b> might be a column
  // storing the pre-computed result a subselect
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, div_(b, 2.0f)), {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, sub_(c, 31)), {0, std::nullopt, 1, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, InSelectUncorrelatedWithoutPrecalculated) {
  // PQP that returns the column "a"
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto pqp_a =
      std::make_shared<Projection>(table_wrapper_a, expression_vector(PQPColumnExpression::from_table(*table_a, "a")));
  const auto select_a = pqp_select_(pqp_a, DataType::Int, false);

  // PQP that returns the column "c"
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto pqp_b =
      std::make_shared<Projection>(table_wrapper_b, expression_vector(PQPColumnExpression::from_table(*table_a, "c")));
  const auto select_b = pqp_select_(pqp_b, DataType::Int, true);

  // Test it without pre-calculated uncorrelated_select_results
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(6, select_a), {0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, select_a), {1, 1, 1, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(add_(a, 2), select_a), {1, 1, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(b, select_a), {1, 1, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34, select_b), {1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34.0, select_b), {1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34.5, select_b), {std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_("hello", select_b), {0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(c, select_b), {1, std::nullopt, 1, std::nullopt}));
}

TEST_F(ExpressionEvaluatorTest, InSelectUncorrelatedWithPrecalculated) {
  // PQP that returns the column "a"
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto pqp_a =
      std::make_shared<Projection>(table_wrapper_a, expression_vector(PQPColumnExpression::from_table(*table_a, "a")));
  const auto select_a = pqp_select_(pqp_a, DataType::Int, false);

  // PQP that returns the column "c"
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto pqp_b =
      std::make_shared<Projection>(table_wrapper_b, expression_vector(PQPColumnExpression::from_table(*table_a, "c")));
  const auto select_b = pqp_select_(pqp_b, DataType::Int, true);

  // Test it with pre-calculated uncorrelated_select_results
  auto uncorrelated_select_results = std::make_shared<ExpressionEvaluator::UncorrelatedSelectResults>();
  table_wrapper_a->execute();
  pqp_a->execute();
  table_wrapper_b->execute();
  pqp_b->execute();
  uncorrelated_select_results->emplace(pqp_a, pqp_a->get_output());
  uncorrelated_select_results->emplace(pqp_b, pqp_b->get_output());

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(6, select_a), {0}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(a, select_a), {1, 1, 1, 1}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(add_(a, 2), select_a), {1, 1, 0, 0}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(b, select_a), {1, 1, 1, 0}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34, select_b), {1}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34.0, select_b), {1}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34.5, select_b), {std::nullopt}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_("hello", select_b), {0}, uncorrelated_select_results));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(c, select_b), {1, std::nullopt, 1, std::nullopt},
                                       uncorrelated_select_results));
}

TEST_F(ExpressionEvaluatorTest, InSelectUncorrelatedWithBrokenPrecalculated) {
  // Make sure the expression evaluator complains if it has been given a list of preevaluated selects but one is missing
  if (!IS_DEBUG) return;

  // PQP that returns the column "a"
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto pqp_a =
      std::make_shared<Projection>(table_wrapper_a, expression_vector(PQPColumnExpression::from_table(*table_a, "a")));
  const auto select_a = pqp_select_(pqp_a, DataType::Int, false);

  // PQP that returns the column "c"
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto pqp_b =
      std::make_shared<Projection>(table_wrapper_b, expression_vector(PQPColumnExpression::from_table(*table_a, "c")));
  const auto select_b = pqp_select_(pqp_b, DataType::Int, true);

  auto uncorrelated_select_results = std::make_shared<ExpressionEvaluator::UncorrelatedSelectResults>();
  table_wrapper_a->execute();
  pqp_a->execute();
  table_wrapper_b->execute();
  pqp_b->execute();
  uncorrelated_select_results->emplace(pqp_a, pqp_a->get_output());
  uncorrelated_select_results->emplace(pqp_b, pqp_b->get_output());

  const auto table_wrapper_c = std::make_shared<TableWrapper>(table_a);
  table_wrapper_c->execute();
  const auto table_scan_c =
      std::make_shared<TableScan>(table_wrapper_c, OperatorScanPredicate(ColumnID{0}, PredicateCondition::Equals, 3));
  table_scan_c->execute();
  const auto projection_c =
      std::make_shared<Projection>(table_scan_c, expression_vector(PQPColumnExpression::from_table(*table_a, "b")));
  projection_c->execute();
  const auto select_c = pqp_select_(projection_c, DataType::Int, true);
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(3, select_c), {0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(4, select_c), {1}));
  EXPECT_THROW(test_expression<int32_t>(table_a, *in_(4, select_c), {0}, uncorrelated_select_results),
               std::logic_error);
}

TEST_F(ExpressionEvaluatorTest, InSelectCorrelated) {
  // PQP that returns the column "b" multiplied with the current value in "a"
  //
  // row   list returned from select
  //  0      (1, 2, 3, 4)
  //  1      (2, 4, 6, 8)
  //  2      (3, 6, 9, 12)
  //  3      (4, 8, 12, 16)
  const auto table_wrapper_a = std::make_shared<TableWrapper>(table_a);
  const auto mul_a = mul_(uncorrelated_parameter_(ParameterID{0}), PQPColumnExpression::from_table(*table_a, "a"));
  const auto pqp_a = std::make_shared<Projection>(table_wrapper_a, expression_vector(mul_a));
  const auto select_a = pqp_select_(pqp_a, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(4, select_a), {1, 1, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(6, select_a), {0, 1, 1, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(16, select_a), {0, 0, 0, 1}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(b, select_a), {1, 0, 0, 0}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(null_(), select_a),
                                       {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));

  // PQP that returns the column "c" added to the current value in "a"
  //
  // row   list returned from select
  //  0      (34, NULL, 35, NULL)
  //  1      (35, NULL, 36, NULL)
  //  2      (36, NULL, 37, NULL)
  //  3      (37, NULL, 38, NULL)
  const auto table_wrapper_b = std::make_shared<TableWrapper>(table_a);
  const auto add_b = add_(uncorrelated_parameter_(ParameterID{0}), PQPColumnExpression::from_table(*table_a, "c"));
  const auto pqp_b = std::make_shared<Projection>(table_wrapper_b, expression_vector(add_b));
  const auto select_b = pqp_select_(pqp_b, DataType::Int, true, std::make_pair(ParameterID{0}, ColumnID{0}));

  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(34, select_b), {1, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(35, select_b), {1, 1, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(null_(), select_b),
                                       {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(36, select_b), {std::nullopt, 1, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(36.0, select_b), {std::nullopt, 1, 1, std::nullopt}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *in_(36.3, select_b),
                                       {std::nullopt, std::nullopt, std::nullopt, std::nullopt}));
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
  const auto parameter_a = uncorrelated_parameter_(ParameterID{0});
  const auto a_plus_x_projection =
      std::make_shared<Projection>(table_wrapper, expression_vector(add_(parameter_a, x), x));
  const auto a_plus_x_eq_13_scan = std::make_shared<TableScan>(
      a_plus_x_projection, OperatorScanPredicate{ColumnID{0}, PredicateCondition::Equals, 13});
  const auto pqp_select_expression =
      pqp_select_(a_plus_x_eq_13_scan, DataType::Int, false, std::make_pair(ParameterID{0}, ColumnID{0}));

  const auto exists_expression = std::make_shared<ExistsExpression>(pqp_select_expression);
  EXPECT_TRUE(test_expression<int32_t>(table_a, *exists_expression, {0, 0, 1, 1}));

  EXPECT_EQ(exists_expression->data_type(), ExpressionEvaluator::DataTypeBool);
  EXPECT_FALSE(exists_expression->is_nullable());
}

TEST_F(ExpressionEvaluatorTest, ExtractLiterals) {
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Year, "1992-09-30"), {"1992"}));
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Month, "1992-09-30"), {"09"}));
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Day, "1992-09-30"), {"30"}));
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Year, null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Month, null_()), {std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(*extract_(DatetimeComponent::Day, null_()), {std::nullopt}));

  EXPECT_THROW(test_expression<std::string>(*extract_(DatetimeComponent::Hour, "1992-09-30"), {"30"}),
               std::logic_error);
  EXPECT_THROW(test_expression<std::string>(*extract_(DatetimeComponent::Minute, "1992-09-30"), {"30"}),
               std::logic_error);
  EXPECT_THROW(test_expression<std::string>(*extract_(DatetimeComponent::Second, "1992-09-30"), {"30"}),
               std::logic_error);

  EXPECT_EQ(extract_(DatetimeComponent::Year, "1993-08-01")->data_type(), DataType::String);
}

TEST_F(ExpressionEvaluatorTest, ExtractSeries) {
  EXPECT_TRUE(test_expression<std::string>(table_a, *extract_(DatetimeComponent::Year, dates),
                                           {"2017", "2014", "2011", "2010"}));
  EXPECT_TRUE(
      test_expression<std::string>(table_a, *extract_(DatetimeComponent::Month, dates), {"12", "08", "09", "01"}));
  EXPECT_TRUE(
      test_expression<std::string>(table_a, *extract_(DatetimeComponent::Day, dates), {"06", "05", "03", "02"}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *extract_(DatetimeComponent::Year, dates2),
                                           {"2017", "2014", std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *extract_(DatetimeComponent::Month, dates2),
                                           {"12", "08", std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *extract_(DatetimeComponent::Day, dates2),
                                           {"06", "05", std::nullopt, std::nullopt}));
  EXPECT_TRUE(test_expression<std::string>(table_empty, *extract_(DatetimeComponent::Day, empty_s), {}));
}

TEST_F(ExpressionEvaluatorTest, CastLiterals) {
  EXPECT_TRUE(test_expression<int32_t>(*cast_(5.5, DataType::Int), {5}));
  EXPECT_TRUE(test_expression<float>(*cast_(5.5, DataType::Float), {5.5f}));
  EXPECT_TRUE(test_expression<float>(*cast_(5, DataType::Float), {5.0f}));
  EXPECT_TRUE(test_expression<std::string>(*cast_(5.5, DataType::String), {"5.5"}));
  EXPECT_TRUE(test_expression<int32_t>(*cast_(null_(), DataType::Int), {std::nullopt}));

  // Following SQLite, CAST("Hello" AS INT) yields zero
  EXPECT_TRUE(test_expression<int32_t>(*cast_("Hello", DataType::Int), {0}));
  EXPECT_TRUE(test_expression<float>(*cast_("Hello", DataType::Float), {0.0f}));
}

TEST_F(ExpressionEvaluatorTest, CastSeries) {
  EXPECT_TRUE(test_expression<int32_t>(table_a, *cast_(a, DataType::Int), {1, 2, 3, 4}));
  EXPECT_TRUE(test_expression<float>(table_a, *cast_(a, DataType::Float), {1.0f, 2.0f, 3.0f, 4.0f}));
  EXPECT_TRUE(test_expression<std::string>(table_a, *cast_(a, DataType::String), {"1", "2", "3", "4"}));
  EXPECT_TRUE(test_expression<int32_t>(table_a, *cast_(f, DataType::Int), {99, 2, 13, 15}));
  EXPECT_TRUE(
      test_expression<std::string>(table_a, *cast_(c, DataType::String), {"33", std::nullopt, "34", std::nullopt}));
}

}  // namespace opossum
