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

    rule = std::make_shared<LogicalReductionRule>();
  }

  std::shared_ptr<AbstractExpression> reduce_distributivity(const std::shared_ptr<AbstractExpression>& expression) {
    return LogicalReductionRule::reduce_distributivity(expression);
  }

  std::shared_ptr<MockNode> mock_node;
  std::shared_ptr<AbstractExpression> a, b, c, d, e;
  std::shared_ptr<AbstractExpression> a2, b2, c2, d2, e2;
  std::shared_ptr<LogicalReductionRule> rule;
};

TEST_F(LogicalReductionRuleTest, ReduceDistributivity) {
  EXPECT_EQ(*reduce_distributivity(or_(and_(a, b), and_(a2, b2))), *and_(a, b));
  EXPECT_EQ(*reduce_distributivity(or_(and_(a2, b), b2)), *and_(b, a2));

  // clang-format off

  // (a AND c AND b) OR (d AND a AND e) -->
  // a AND (c AND b) OR (e AND d)
  EXPECT_EQ(*reduce_distributivity(or_(and_(a, and_(c, b)), and_(and_(d2, a2), e2))),
                                  *and_(a, or_(and_(c, b), and_(e, d))));

  // (a AND b AND c) OR (a AND b) OR (a AND d AND b AND a) OR (b AND a AND e) -->
  // a AND b AND (c OR d OR e)
  EXPECT_EQ(*reduce_distributivity(or_(or_(and_(a2, and_(b, c)), and_(a, b)), or_(and_(and_(and_(a, d2), b), a), and_(b2, and_(a, e))))),  // NOLINT
            *and_(and_(a2, b), or_(or_(c, d), e)));

  // clang-format on

  // Expressions that aren't semantically modified
  EXPECT_EQ(*reduce_distributivity(or_(and_(a2, b), or_(and_(c, d), c))), *or_(or_(and_(a2, b), and_(c, d)), c));
  EXPECT_EQ(*reduce_distributivity(and_(or_(a2, b), and_(a2, b))), *and_(and_(or_(a, b), a), b));
  EXPECT_EQ(*reduce_distributivity(a), *a);
  EXPECT_EQ(*reduce_distributivity(and_(a, b)), *and_(a, b));
  EXPECT_EQ(*reduce_distributivity(or_(a, b)), *or_(a, b));
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
