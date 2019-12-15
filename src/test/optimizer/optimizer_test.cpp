#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}}, "node_a");
    a = node_a->get_column("a");
    b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}}, "node_b");
    x = node_b->get_column("x");
    y = node_b->get_column("y");

    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}}, "node_c");
    u = node_c->get_column("u");

    subquery_lqp_a = LimitNode::make(to_expression(1), node_b);
    subquery_a = lqp_subquery_(subquery_lqp_a);
    subquery_lqp_b = LimitNode::make(to_expression(1), PredicateNode::make(greater_than_(u, 3), node_c));
    subquery_b = lqp_subquery_(subquery_lqp_b);
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c;
  LQPColumnReference a, b, x, y, u;
  std::shared_ptr<AbstractLQPNode> subquery_lqp_a, subquery_lqp_b;
  std::shared_ptr<LQPSubqueryExpression> subquery_a, subquery_b;
};

TEST_F(OptimizerTest, RequiresOwnership) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto optimizer = Optimizer::create_default_optimizer();
  // Does not move the LQP into the optimizer
  EXPECT_THROW(optimizer->optimize(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsValidOutputs) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto out_of_plan_node = LimitNode::make(value_(10), lqp->left_input());

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsCorrectNumberOfInputs) {
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(b, 1),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a)));
  // clang-format on

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, AssertsInPlanReferences) {
  // clang-format off
  auto lqp =
  JoinNode::make(JoinMode::Inner, equals_(x, 1),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a)));
  // clang-format on

  EXPECT_THROW(Optimizer::validate_lqp(lqp), std::logic_error);
}

TEST_F(OptimizerTest, VerifiesResults) {
  if (!HYRISE_DEBUG) GTEST_SKIP();
  // While the Asserts* tests checked the different features of validate_lqp, this test checks that a rule that breaks
  // an LQP throws an assertion.
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  Optimizer optimizer{};

  class LQPBreakingRule : public AbstractRule {
   public:
    explicit LQPBreakingRule(const std::shared_ptr<AbstractExpression>& out_of_plan_expression)
        : out_of_plan_expression(out_of_plan_expression) {}

    void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override {
      // Change the `b` expression in the projection to `x`, which is not part of the input LQP
      const auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(root->left_input());
      if (!projection_node) return;
      projection_node->node_expressions[0] = out_of_plan_expression;
    }

    std::shared_ptr<AbstractExpression> out_of_plan_expression;
  };

  optimizer.add_rule(std::make_unique<LQPBreakingRule>(lqp_column_(x)));

  EXPECT_THROW(optimizer.optimize(std::move(lqp)), std::logic_error);
}

TEST_F(OptimizerTest, OptimizesSubqueries) {
  /**
   * Test that the Optimizer's rules reach subqueries
   */

  std::unordered_set<std::shared_ptr<AbstractLQPNode>> nodes;

  // A "rule" that just collects the nodes it was applied to
  class MockRule : public AbstractRule {
   public:
    explicit MockRule(std::unordered_set<std::shared_ptr<AbstractLQPNode>>& nodes) : nodes(nodes) {}

    void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override {
      nodes.emplace(root);
      _apply_to_inputs(root);
    }

    std::unordered_set<std::shared_ptr<AbstractLQPNode>>& nodes;
  };

  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(b, subquery_a)),
    PredicateNode::make(greater_than_(a, subquery_b),
      node_a));
  // clang-format on

  auto rule = std::make_unique<MockRule>(nodes);

  Optimizer optimizer{};
  optimizer.add_rule(std::move(rule));

  optimizer.optimize(std::move(lqp));

  // Test that the optimizer has reached all nodes (the number includes all nodes created above and the root nodes
  // created by the optimizer for the lqp and each subquery)
  EXPECT_EQ(nodes.size(), 11u);
}

TEST_F(OptimizerTest, OptimizesSubqueriesExactlyOnce) {
  /**
   * This one is important. An LQP can contain the same Subquery multiple times (see the LQP constructed below).
   * We need make sure that each LQP is only optimized once and that all SubqueryExpressions point to the same LQP after
   * optimization.
   * 
   * This is not just "nice to have". If we do not do this properly and multiple SubqueryExpressions point to the same LQP
   * then we would potentially break all the other SubqueryExpressions while moving nodes around when optimizing a single
   * one of them.
   */

  // clang-format off
  /** Initialise an LQP that contains the same sub query expression twice */
  auto lqp = std::static_pointer_cast<AbstractLQPNode>(
  PredicateNode::make(greater_than_(add_(b, subquery_a), 2),
    ProjectionNode::make(expression_vector(add_(b, subquery_a)),
      PredicateNode::make(greater_than_(a, subquery_b),
        node_a))));
  // clang-format on

  /**
   * 1. Optional: Deep copy the LQP, thereby making its SubqueryExpressions point to different copies of the same LQP
   *    (The optimizer should incidentally make them point to the same LQP again)
   */
  lqp = lqp->deep_copy();
  {
    auto predicate_node_a = std::dynamic_pointer_cast<PredicateNode>(lqp);
    ASSERT_TRUE(predicate_node_a);
    auto subquery_a_a = std::dynamic_pointer_cast<LQPSubqueryExpression>(
        predicate_node_a->predicate()->arguments.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_a);
    auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(lqp->left_input());
    ASSERT_TRUE(projection_node);
    auto subquery_a_b =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(projection_node->node_expressions.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_b);

    EXPECT_NE(subquery_a_a->lqp, subquery_a_b->lqp);
  }

  /**q
   * 2. Optimize the LQP with a telemetric Rule
   */

  // A "rule" that counts how often it was `apply_to()`ed. We use this
  // to check whether SubqueryExpressions pointing to the same LQP before optimization still point to the same (though
  // deep_copy()ed) LQP afterwards
  size_t counter{0};
  class MockRule : public AbstractRule {
   public:
    explicit MockRule(size_t& counter) : counter(counter) {}

    void apply_to(const std::shared_ptr<AbstractLQPNode>& root) const override { ++counter; }

    size_t& counter;
  };

  auto rule = std::make_unique<MockRule>(counter);

  Optimizer optimizer{};
  optimizer.add_rule(std::move(rule));

  const auto optimized_lqp = optimizer.optimize(std::move(lqp));
  lqp = nullptr;

  /**
   * 3. Check that the rule was invoked 3 times. Once for the main LQP, once for subquery_a and once for subquery_b
   */
  EXPECT_EQ(counter, 3u);

  /**
   * 4. Check that now - after optimizing - both SubqueryExpressions using subquery_lqp_a point to the same LQP object
   * again
   */
  {
    auto predicate_node_a = std::dynamic_pointer_cast<PredicateNode>(optimized_lqp);
    ASSERT_TRUE(predicate_node_a);
    auto subquery_a_a = std::dynamic_pointer_cast<LQPSubqueryExpression>(
        predicate_node_a->predicate()->arguments.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_a);
    auto projection_node = std::dynamic_pointer_cast<ProjectionNode>(optimized_lqp->left_input());
    ASSERT_TRUE(projection_node);
    auto subquery_a_b =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(projection_node->node_expressions.at(0)->arguments.at(1));
    ASSERT_TRUE(subquery_a_b);
    auto predicate_node_b = std::dynamic_pointer_cast<PredicateNode>(optimized_lqp->left_input()->left_input());
    ASSERT_TRUE(predicate_node_b);
    auto subquery_b_a =
        std::dynamic_pointer_cast<LQPSubqueryExpression>(predicate_node_b->predicate()->arguments.at(1));
    ASSERT_TRUE(subquery_b_a);

    EXPECT_EQ(subquery_a_a->lqp, subquery_a_b->lqp);
    EXPECT_LQP_EQ(subquery_b_a->lqp, subquery_lqp_b);
  }
}

}  // namespace opossum
