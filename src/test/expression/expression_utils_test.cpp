#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionUtilsTest : public BaseTest {
 public:
  void SetUp() override {
    node_a =
        MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}});
    a_a = LQPColumnReference{node_a, ColumnID{0}};
    a_b = LQPColumnReference{node_a, ColumnID{1}};
    a_c = LQPColumnReference{node_a, ColumnID{2}};

    node_b = MockNode::make(MockNode::ColumnDefinitions{{{DataType::Int, "a"}, {DataType::Int, "b"}}});
    b_a = LQPColumnReference{node_b, ColumnID{0}};
    b_b = LQPColumnReference{node_b, ColumnID{1}};
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_a, a_b, a_c, b_a, b_b;
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
  EXPECT_TRUE(expression_evaluable_on_lqp(lqp_column_(a_a), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(lqp_column_(a_b), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(lqp_column_(b_a), *input_lqp));

  // Expressions that can be computed using a projection
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(lqp_column_(a_a), 1), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(lqp_column_(a_b), lqp_column_(a_c)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(add_(lqp_column_(b_a), lqp_column_(a_a)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(mul_(lqp_column_(b_a), 2), *input_lqp));

  // Expressions that can be computed using an aggregate
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(lqp_column_(a_c)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(sum_(lqp_column_(b_a)), *input_lqp));

  // COUNT(*) is always evaluable if the original node is present
  EXPECT_TRUE(
      expression_evaluable_on_lqp(count_(lqp_column_(LQPColumnReference{node_a, INVALID_COLUMN_ID})), *input_lqp));
  EXPECT_FALSE(
      expression_evaluable_on_lqp(count_(lqp_column_(LQPColumnReference{node_b, INVALID_COLUMN_ID})), *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionEvaluableOnLQPAggregate) {
  // SELECT b, SUM(c) FROM a WHERE a = 1
  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(lqp_column_(a_b)), expression_vector(sum_(lqp_column_(a_c))),
    PredicateNode::make(equals_(a_a, 1),
      node_a));
  // clang-format on

  EXPECT_FALSE(expression_evaluable_on_lqp(lqp_column_(a_a), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(lqp_column_(a_b), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(lqp_column_(a_c)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(sum_(lqp_column_(a_a)), *input_lqp));
  EXPECT_FALSE(expression_evaluable_on_lqp(lqp_column_(b_a), *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionEvaluableOnJoin) {
  // SELECT * FROM a, b WHERE a.a = b.a AND a.a = 1
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(lqp_column_(a_a), lqp_column_(b_a)),
    PredicateNode::make(equals_(a_a, 1),
      node_a),
    node_b);
  // clang-format on

  EXPECT_TRUE(expression_evaluable_on_lqp(lqp_column_(a_a), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(lqp_column_(b_a), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(add_(lqp_column_(a_c), lqp_column_(b_b)), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(lqp_column_(a_c)), *input_lqp));
  EXPECT_TRUE(expression_evaluable_on_lqp(sum_(add_(lqp_column_(a_c), lqp_column_(b_b))), *input_lqp));
}

TEST_F(ExpressionUtilsTest, ExpressionDeepReplace) {
  // (a > 5 AND a < 6) AND c = 7
  std::shared_ptr<AbstractExpression> expression =
      and_(and_(greater_than_(a_a, 5), less_than_(a_a, 6)), equals_(a_c, 7));

  // replace a with b; 7 with 8
  const auto mapping = ExpressionUnorderedMap<std::shared_ptr<AbstractExpression>>{{lqp_column_(a_a), lqp_column_(a_b)},
                                                                                   {value_(7), value_(8)}};

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
      and_(greater_than_(a_a, correlated_parameter_(ParameterID{5}, lqp_column_(a_a))), equals_(a_c, 7))));
}

TEST_F(ExpressionUtilsTest, ExpressionContainsCorrelatedParameter) {
  EXPECT_FALSE(expression_contains_correlated_parameter(and_(greater_than_(a_a, 5), equals_(a_c, 7))));
  EXPECT_FALSE(expression_contains_correlated_parameter(
      and_(greater_than_(a_a, placeholder_(ParameterID{5})), equals_(a_c, 7))));
  EXPECT_TRUE(expression_contains_correlated_parameter(
      and_(greater_than_(a_a, correlated_parameter_(ParameterID{5}, lqp_column_(a_a))), equals_(a_c, 7))));
}

}  // namespace opossum
