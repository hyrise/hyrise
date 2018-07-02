#include "gtest/gtest.h"

#include "expression/expression_factory.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"

using namespace opossum::expression_factory;  // NOLINT

namespace opossum {

class OptimizerTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}});
    a = node_a->get_column("a");
    b = node_a->get_column("b");

    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}});
    x = node_b->get_column("x");
    y = node_b->get_column("y");
  }

  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a, b, x, y;
};

TEST_F(OptimizerTest, RuleBatches) {
  struct MockRule : public AbstractRule {
    explicit MockRule(size_t num_iterations) : num_iterations(num_iterations) {}

    std::string name() const override { return "MockNode"; }

    bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override {
      num_iterations = num_iterations > 0 ? num_iterations - 1 : 0;
      return num_iterations != 0;
    }

    uint32_t num_iterations;
  };

  auto iterative_rule_a = std::make_shared<MockRule>(4u);
  auto iterative_rule_b = std::make_shared<MockRule>(8u);
  auto iterative_rule_c = std::make_shared<MockRule>(12u);
  auto iterative_rule_d = std::make_shared<MockRule>(7u);

  RuleBatch iterative_batch(RuleBatchExecutionPolicy::Iterative);
  iterative_batch.add_rule(iterative_rule_a);
  iterative_batch.add_rule(iterative_rule_b);
  iterative_batch.add_rule(iterative_rule_c);

  RuleBatch once_batch(RuleBatchExecutionPolicy::Once);
  once_batch.add_rule(iterative_rule_d);

  Optimizer optimizer{10};
  optimizer.add_rule_batch(iterative_batch);
  optimizer.add_rule_batch(once_batch);

  auto lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});

  optimizer.optimize(lqp);

  EXPECT_EQ(iterative_rule_a->num_iterations, 0u);
  EXPECT_EQ(iterative_rule_b->num_iterations, 0u);
  EXPECT_EQ(iterative_rule_c->num_iterations, 2u);

  EXPECT_EQ(iterative_rule_d->num_iterations, 6u);
}

TEST_F(OptimizerTest, OptimizesSubqueries) {
  /**
   * Test that the Optimizer's rules reach subqueries
   */

  // A "rule" that just collects the nodes it was applied to
  class MockRule : public AbstractRule {
   public:
    std::string name() const override { return "Mock"; }

    bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override {
      nodes.emplace(root);
      _apply_recursively(root);
      return false;
    }

    std::unordered_set<std::shared_ptr<AbstractLQPNode>> nodes;
  };

  // clang-format off
  const auto subselect_lqp_a = LimitNode::make(to_expression(1), node_b);
  const auto subselect_a = select(subselect_lqp_a);
  const auto subselect_lqp_b = LimitNode::make(to_expression(1), PredicateNode::make(greater_than(x, y), node_b));
  const auto subselect_b = select(subselect_lqp_b);

  const auto lqp =
  ProjectionNode::make(expression_vector(add(b, subselect_a)),
    PredicateNode::make(greater_than(a, subselect_b),
      node_a
  ));
  // clang-format on

  const auto rule = std::make_shared<MockRule>();

  Optimizer optimizer{1};
  RuleBatch once_batch(RuleBatchExecutionPolicy::Once);
  once_batch.add_rule(rule);
  optimizer.add_rule_batch(once_batch);

  optimizer.optimize(lqp);

  // Test that the optimizer has reached all nodes (the number includes all nodes created above and the root node
  // created by the optimizer)
  EXPECT_EQ(rule->nodes.size(), 8u);
}

}  // namespace opossum
