#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/strategy/logical_reduction_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LogicalReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    mock_node = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}, {DataType::Int, "e"}});
    a = equals_(mock_node->get_column("a"), 0);
    b = equals_(mock_node->get_column("b"), 0);
    c = equals_(mock_node->get_column("c"), 0);
    d = equals_(mock_node->get_column("d"), 0);
    e = equals_(mock_node->get_column("e"), 0);

    rule = std::make_shared<LogicalReductionRule>();
  }

  std::shared_ptr<AbstractExpression> reduce_distributivity(const std::shared_ptr<AbstractExpression>& expression) {
    return LogicalReductionRule::reduce_distributivity(expression);
  }

  std::shared_ptr<MockNode> mock_node;
  std::shared_ptr<AbstractExpression> a, b, c, d, e;
  std::shared_ptr<LogicalReductionRule> rule;
};

TEST_F(LogicalReductionRuleTest, ReduceDistributivity) {
  EXPECT_EQ(*reduce_distributivity(a), *a);
  EXPECT_EQ(*reduce_distributivity(and_(a, b)), *and_(a, b));
  EXPECT_EQ(*reduce_distributivity(or_(a, b)), *or_(a, b));
  EXPECT_EQ(*reduce_distributivity(or_(and_(a, b), and_(a, b))), *and_(a, b));
  // clang-format off

  // (a AND c AND b) OR (d AND a AND e) -->
  // a AND (c AND b) OR (e AND d)
  EXPECT_EQ(*reduce_distributivity(or_(and_(a, and_(c, b)), and_(and_(d, a), e))),
                                  *and_(a, or_(and_(c, b), and_(e, d))));

  // (a AND b AND c) OR (a AND b) OR (a AND d AND b AND a) OR (b AND a AND e) -->
  // a AND b AND (c OR d OR e)
  EXPECT_EQ(*reduce_distributivity(or_(or_(and_(a, and_(b, c)), and_(a, b)), or_(and_(and_(and_(a, d), b), a), and_(b, and_(a, e))))),  // NOLINT
            *and_(and_(a, b), or_(or_(c, d), e)));

  // clang-format on
}

TEST_F(LogicalReductionRuleTest, ApplyToProjection) {
  // (a AND b) OR (a AND c) -> a AND (c OR b)
  // (a AND b) AND (a AND c) -> no change

  const auto a_and_b = and_(a, b);
  const auto a_and_c = and_(a, c);
  const auto expressions = expression_vector(or_(a_and_b, a_and_c), and_(a_and_b, a_and_c));

  const auto input_lqp = ProjectionNode::make(expressions, mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);

  // clang-format off
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(and_(a, or_(b, c)), and_(and_(a_and_b, a), c)),
    mock_node);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(LogicalReductionRuleTest, ApplyToPredicate) {
  // (a AND b) OR (a AND c) -> PredicateNode{a} -> PredicateNode{b OR c}

  const auto a_and_b = and_(a, b);
  const auto a_and_c = and_(a, c);

  const auto input_lqp = PredicateNode::make(or_(a_and_b, a_and_c), mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(or_(b, c),
    PredicateNode::make(a,
      mock_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
