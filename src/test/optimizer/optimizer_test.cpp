#include "gtest/gtest.h"

#include "logical_query_plan/mock_node.hpp"
#include "optimizer/optimizer.hpp"
#include "optimizer/strategy/abstract_rule.hpp"

namespace opossum {

class OptimizerTest : public ::testing::Test {};

struct MockRule : public AbstractRule {
  explicit MockRule(size_t num_iterations) : num_iterations(num_iterations) {}

  std::string name() const override { return "MockNode"; }

  bool apply_to(const std::shared_ptr<AbstractLQPNode>& root) override {
    num_iterations = num_iterations > 0 ? num_iterations - 1 : 0;
    return num_iterations != 0;
  }

  uint32_t num_iterations;
};

TEST_F(OptimizerTest, RuleBatches) {
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

}  // namespace opossum
