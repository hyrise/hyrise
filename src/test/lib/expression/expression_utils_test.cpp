#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/pqp_subquery_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "operators/get_table.hpp"
#include "operators/projection.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class ExpressionUtilsTest : public BaseTest {
 public:
  void SetUp() override {
    node_a =
        MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}});
    a_a = node_a->get_column("a");
    a_b = node_a->get_column("b");
    a_c = node_a->get_column("c");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}}});
    b_a = node_b->get_column("a");
    b_b = node_b->get_column("b");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LQPColumnExpression> a_a, a_b, a_c, b_a, b_b;
};

TEST_F(ExpressionUtilsTest, ExpressionFlattenAndInflate) {
  // a > 5 AND b < 6 AND c = 7
  const auto expression = and_(and_(greater_than_(a_a, 5), less_than_(a_b, 6)), equals_(a_c, 7));
  const auto flattened_expressions = flatten_logical_expressions(expression, LogicalOperator::And);

  ASSERT_EQ(flattened_expressions.size(), 3u);
  EXPECT_EQ(*flattened_expressions.at(0), *equals_(a_c, 7));
  EXPECT_EQ(*flattened_expressions.at(1), *greater_than_(a_a, 5));
  EXPECT_EQ(*flattened_expressions.at(2), *less_than_(a_b, 6));

  const auto inflated_expression = inflate_logical_expressions(flattened_expressions, LogicalOperator::Or);
  EXPECT_EQ(*inflated_expression, *or_(or_(equals_(a_c, 7), greater_than_(a_a, 5)), less_than_(a_b, 6)));
}

TEST_F(ExpressionUtilsTest, ExpressionEvaluableOnLQPSimple) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a_a, 1),
    node_a);
  // clang-format on

  // Expressions that are already available as columns
  EXPECT_TRUE(expression_evaluable_on_lqp(a_a, *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(a_b, *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(b_a, *input_lqp));

  // Expressions that can be computed using a projection
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(a_a, 1), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(a_b, a_c), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(add_(b_a, a_a), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(mul_(b_a, 2), *input_lqp));

  // Expressions that can be computed using an aggregate
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(a_c), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(sum_(b_a), *input_lqp));

  // COUNT(*) is always evaluable if the original node is present
  EXPECT_TRUE(expression_evaluable_on_lqp(count_(lqp_column_(node_a, INVALID_COLUMN_ID)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(count_(lqp_column_(node_b, INVALID_COLUMN_ID)), *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionEvaluableOnLQPAggregate) {
  // SELECT b, SUM(c) FROM a WHERE a = 1
  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_b), expression_vector(sum_(a_c)),
    PredicateNode::make(equals_(a_a, 1),
      node_a));
  // clang-format on

  EXPECT_FALSE(expression_evaluable_on_lqp(a_a, *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(a_b, *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(a_c), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(sum_(a_a), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(b_a, *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionEvaluableOnJoin) {
  // SELECT * FROM a, b WHERE a.a = b.a AND a.a = 1
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
    PredicateNode::make(equals_(a_a, 1),
      node_a),
    node_b);
  // clang-format on

  EXPECT_TRUE(expression_evaluable_on_lqp(a_a, *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(b_a, *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(a_c, b_b), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(a_c), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(add_(a_c, b_b)), *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionDeepReplace) {
  // (a > 5 AND a < 6) AND c = 7
  std::shared_ptr<AbstractExpression> expression =
      and_(and_(greater_than_(a_a, 5), less_than_(a_a, 6)), equals_(a_c, 7));

  // replace a with b; 7 with 8
  const auto mapping = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>{{a_a, a_b}, {value_(7), value_(8)}};

  expression_deep_replace(expression, mapping);

  EXPECT_EQ(*expression, *and_(and_(greater_than_(a_b, 5), less_than_(a_b, 6)), equals_(a_c, 8)));
}

TEST_F(ExpressionUtilsTest, ExpressionCommonType) {
  EXPECT_EQ(expression_common_type(DataType::Int, DataType::Int), DataType::Int);
  EXPECT_EQ(expression_common_type(DataType::Int, DataType::Float), DataType::Float);
  EXPECT_EQ(expression_common_type(DataType::Int, DataType::Null), DataType::Int);
  EXPECT_EQ(expression_common_type(DataType::Float, DataType::Float), DataType::Float);
  EXPECT_EQ(expression_common_type(DataType::Null, DataType::Float), DataType::Float);
  EXPECT_EQ(expression_common_type(DataType::Double, DataType::Int), DataType::Double);
  EXPECT_EQ(expression_common_type(DataType::Double, DataType::Double), DataType::Double);
  EXPECT_EQ(expression_common_type(DataType::Float, DataType::Double), DataType::Double);
  EXPECT_EQ(expression_common_type(DataType::Long, DataType::Long), DataType::Long);
  EXPECT_EQ(expression_common_type(DataType::String, DataType::String), DataType::String);
}

TEST_F(ExpressionUtilsTest, ExpressionContainsPlaceholders) {
  EXPECT_FALSE(expression_contains_placeholder(and_(greater_than_(a_a, 5), equals_(a_c, 7))));
  EXPECT_TRUE(expression_contains_placeholder(and_(greater_than_(a_a, placeholder_(ParameterID{5})), equals_(a_c, 7))));
  EXPECT_FALSE(expression_contains_placeholder(
      and_(greater_than_(a_a, correlated_parameter_(ParameterID{5}, a_a)), equals_(a_c, 7))));
}

TEST_F(ExpressionUtilsTest, ExpressionContainsCorrelatedParameter) {
  EXPECT_FALSE(expression_contains_correlated_parameter(and_(greater_than_(a_a, 5), equals_(a_c, 7))));
  EXPECT_FALSE(expression_contains_correlated_parameter(
      and_(greater_than_(a_a, placeholder_(ParameterID{5})), equals_(a_c, 7))));
  EXPECT_TRUE(expression_contains_correlated_parameter(
      and_(greater_than_(a_a, correlated_parameter_(ParameterID{5}, a_a)), equals_(a_c, 7))));
}

TEST_F(ExpressionUtilsTest, CollectPQPSubqueryExpressionsSingle) {
  const auto pqp_subquery = std::make_shared<PQPSubqueryExpression>(std::make_shared<GetTable>("table_a"));

  auto subqueries = find_pqp_subquery_expressions(pqp_subquery);

  EXPECT_EQ(subqueries.size(), 1);
  EXPECT_EQ(subqueries.at(0), pqp_subquery);
}

TEST_F(ExpressionUtilsTest, CollectPQPSubqueryExpressionsMultiple) {
  const auto pqp_subquery_a = std::make_shared<PQPSubqueryExpression>(std::make_shared<GetTable>("table_a"));
  const auto pqp_subquery_b = std::make_shared<PQPSubqueryExpression>(std::make_shared<GetTable>("table_b"));
  auto expression = add_(pqp_subquery_a, sub_(value_(1), pqp_subquery_b));

  auto subqueries = find_pqp_subquery_expressions(expression);

  EXPECT_EQ(subqueries.size(), 2);
  EXPECT_EQ(subqueries.at(0), pqp_subquery_a);
  EXPECT_EQ(subqueries.at(1), pqp_subquery_b);
}

TEST_F(ExpressionUtilsTest, CollectPQPSubqueryExpressionsIgnoreNested) {
  // (1) Nested Subquery
  const auto nested_subquery = std::make_shared<PQPSubqueryExpression>(std::make_shared<GetTable>("table_a"));
  // (2) Root Subquery
  const auto projection = std::make_shared<Projection>(std::make_shared<GetTable>("table_b"),
                                                       expression_vector(add_(nested_subquery, value_(1))));
  const auto subquery = std::make_shared<PQPSubqueryExpression>(projection);

  // Nested PQPSubqueryExpressions should NOT be returned
  {
    auto found_pqp_subqueries = find_pqp_subquery_expressions(subquery);
    EXPECT_EQ(found_pqp_subqueries.size(), 1);
    EXPECT_EQ(found_pqp_subqueries.at(0), subquery);
  }
  // Wrapping subqueries should not make a difference
  {
    auto wrapped_subquery = add_(subquery, value_(1));
    auto found_pqp_subqueries = find_pqp_subquery_expressions(wrapped_subquery);
    EXPECT_EQ(found_pqp_subqueries.size(), 1);
    EXPECT_EQ(found_pqp_subqueries.at(0), subquery);
  }
}

TEST_F(ExpressionUtilsTest, GetValueOrParameter) {
  const auto expected_value = AllTypeVariant{int64_t{1}};
  const auto value_expression = value_(expected_value);
  const auto correlated_parameter_expression = correlated_parameter_(ParameterID{0}, value_expression);
  correlated_parameter_expression->set_value(expected_value);
  const auto invalid_cast = cast_(value_(pmr_string{"1.2"}), DataType::Int);
  const auto cast_as_null = cast_(expected_value, DataType::Null);
  const auto cast_as_float = cast_(value_expression, DataType::Float);
  const auto cast_from_null = cast_(null_(), DataType::Int);
  const auto cast_column = cast_(a_a, DataType::Float);

  EXPECT_THROW(expression_get_value_or_parameter(*invalid_cast), std::logic_error);
  // Casts as NULL are undefined
  EXPECT_THROW(expression_get_value_or_parameter(*cast_as_null), std::logic_error);

  {
    const auto actual_value = expression_get_value_or_parameter(*value_expression);
    EXPECT_NE(actual_value, std::nullopt);
    EXPECT_EQ(*actual_value, expected_value);
  }
  {
    const auto actual_value = expression_get_value_or_parameter(*correlated_parameter_expression);
    EXPECT_NE(actual_value, std::nullopt);
    EXPECT_EQ(*actual_value, expected_value);
  }
  {
    const auto actual_value = expression_get_value_or_parameter(*cast_as_float);
    EXPECT_NE(actual_value, std::nullopt);
    EXPECT_FLOAT_EQ(boost::get<float>(*actual_value), 1.0);
  }
  {
    // Casts from NULL should return a NULL value
    const auto actual_value = expression_get_value_or_parameter(*cast_from_null);
    EXPECT_NE(actual_value, std::nullopt);
    EXPECT_TRUE(variant_is_null(*actual_value));
  }
  {
    // More complicated casts should be evaluated by ExpressionEvaluator
    const auto actual_value = expression_get_value_or_parameter(*cast_column);
    EXPECT_EQ(actual_value, std::nullopt);
  }
}

TEST_F(ExpressionUtilsTest, FindExpressionIDx) {
  const auto expression_vector = std::vector<std::shared_ptr<AbstractExpression>>{a_a, a_b};

  EXPECT_EQ(find_expression_idx(*a_c, expression_vector), std::nullopt);

  EXPECT_NE(find_expression_idx(*a_a, expression_vector), std::nullopt);
  EXPECT_EQ(*find_expression_idx(*a_a, expression_vector), ColumnID{0});

  EXPECT_NE(find_expression_idx(*a_b, expression_vector), std::nullopt);
  EXPECT_EQ(*find_expression_idx(*a_b, expression_vector), ColumnID{1});

  const auto a_a_copy = a_a->deep_copy();
  EXPECT_NE(find_expression_idx(*a_a_copy, expression_vector), std::nullopt);
  EXPECT_EQ(*find_expression_idx(*a_a_copy, expression_vector), ColumnID{0});
}

}  // namespace hyrise
