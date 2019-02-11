#include <memory>

#include "base_test.hpp"

#include "expression/arithmetic_expression.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/expression_reduction_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExpressionReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    mock_node = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}, {DataType::Int, "e"}});

    // Create two objects for each expression to make sure the algorithm tests for expression equality, not for pointer
    // equality
    a = equals_(mock_node->get_column("a"), 0);
    a2 = equals_(mock_node->get_column("a"), 0);
    b = equals_(mock_node->get_column("b"), 0);
    b2 = equals_(mock_node->get_column("b"), 0);
    c = equals_(mock_node->get_column("c"), 0);
    c2 = equals_(mock_node->get_column("c"), 0);
    d = equals_(mock_node->get_column("d"), 0);
    d2 = equals_(mock_node->get_column("d"), 0);
    e = equals_(mock_node->get_column("e"), 0);
    e2 = equals_(mock_node->get_column("e"), 0);

    rule = std::make_shared<ExpressionReductionRule>();
  }

  std::shared_ptr<MockNode> mock_node;
  std::shared_ptr<AbstractExpression> a, b, c, d, e;
  std::shared_ptr<AbstractExpression> a2, b2, c2, d2, e2;
  std::shared_ptr<ExpressionReductionRule> rule;
};

TEST_F(ExpressionReductionRuleTest, ReduceDistributivity) {
  // clang-format off
  auto expression_a = std::shared_ptr<AbstractExpression>(or_(and_(a, b), and_(a2, b2)));
  auto expression_b = std::shared_ptr<AbstractExpression>(or_(and_(a2, b), b2));
  auto expression_c = std::shared_ptr<AbstractExpression>(or_(and_(a, and_(c, b)), and_(and_(d2, a2), e2)));
  auto expression_d = std::shared_ptr<AbstractExpression>(or_(or_(and_(a2, and_(b, c)), and_(a, b)), or_(and_(and_(and_(a, d2), b), a), and_(b2, and_(a, e)))));  // NOLINT
  auto expression_e = std::shared_ptr<AbstractExpression>(or_(and_(a2, b), or_(and_(c, d), c)));
  auto expression_f = std::shared_ptr<AbstractExpression>(and_(or_(a2, b), and_(a2, b)));
  auto expression_g = std::shared_ptr<AbstractExpression>(a);
  auto expression_h = std::shared_ptr<AbstractExpression>(and_(a, b));
  auto expression_i = std::shared_ptr<AbstractExpression>(or_(a, b));
  auto expression_j = std::shared_ptr<AbstractExpression>(and_(value_(1), or_(and_(a, b), and_(a2, b2))));
  // clang-format on

  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_a), *and_(a, b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_b), *and_(b, a2));

  // (a AND c AND b) OR (d AND a AND e) -->
  // a AND (c AND b) OR (e AND d)
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_c), *and_(a, or_(and_(c, b), and_(e, d))));

  // (a AND b AND c) OR (a AND b) OR (a AND d AND b AND a) OR (b AND a AND e) -->
  // a AND b AND (c OR d OR e)
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_d), *and_(and_(a2, b), or_(or_(c, d), e)));

  // Expressions that aren't modified
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_e), *or_(or_(and_(a2, b), and_(c, d)), c));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_f), *and_(and_(or_(a, b), a), b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_g), *a);
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_h), *and_(a, b));
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_i), *or_(a, b));

  // Reduction works recursively
  EXPECT_EQ(*ExpressionReductionRule::reduce_distributivity(expression_j), *and_(value_(1), and_(a, b)));
}

TEST_F(ExpressionReductionRuleTest, ReduceInWithSingleListElement) {
  auto in_a = std::shared_ptr<AbstractExpression>(in_(5, list_(1)));
  auto in_b = std::shared_ptr<AbstractExpression>(in_(a, list_(5)));
  auto in_c = std::shared_ptr<AbstractExpression>(in_(5, list_(a)));
  auto in_d = std::shared_ptr<AbstractExpression>(in_(5, list_(1, 2)));
  auto nested_in_a = std::shared_ptr<AbstractExpression>(and_(in_(5, list_(1)), value_(0)));
  auto non_in_a = std::shared_ptr<AbstractExpression>(add_(5, 3));

  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(in_a), *equals_(5, 1));
  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(in_b), *equals_(a, 5));
  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(in_c), *equals_(5, a));
  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(nested_in_a), *and_(equals_(5, 1), value_(0)));

  // Test expressions which reduce_in_with_single_list_element() shouldn't alter
  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(in_d), *in_(5, list_(1, 2)));
  EXPECT_EQ(*ExpressionReductionRule::reduce_in_with_single_list_element(non_in_a), *add_(5, 3));
}

TEST_F(ExpressionReductionRuleTest, ReduceConstantExpression) {
  auto expression_a = std::shared_ptr<AbstractExpression>(add_(1, add_(3, 4)));
  ExpressionReductionRule::reduce_constant_expression(expression_a);
  EXPECT_EQ(*expression_a, *value_(8));

  auto expression_b = std::shared_ptr<AbstractExpression>(not_equals_(in_(a, list_(1, 2, add_(5, 3), 4)), 0));
  ExpressionReductionRule::reduce_constant_expression(expression_b);
  EXPECT_EQ(*expression_b, *not_equals_(in_(a, list_(1, 2, 8, 4)), 0));

  auto expression_c = std::shared_ptr<AbstractExpression>(in_(a, list_(5)));
  ExpressionReductionRule::reduce_constant_expression(expression_c);
  EXPECT_EQ(*expression_c, *in_(a, list_(5)));
}

TEST_F(ExpressionReductionRuleTest, ApplyToLQP) {
  const auto a_and_b = and_(a, b);
  const auto a_and_c = and_(a, c);

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(or_(a_and_b, a_and_c),
    PredicateNode::make(in_(a, list_(5)),
      PredicateNode::make(equals_(3, add_(4, 3)),
        mock_node)));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(and_(a, or_(b, c)),
    PredicateNode::make(equals_(a, 5),
      PredicateNode::make(equals_(3, 7),
        mock_node)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
