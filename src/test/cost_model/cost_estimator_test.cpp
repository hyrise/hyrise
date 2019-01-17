#include <unordered_map>

#include "gtest/gtest.h"

#include "cost_model/abstract_cost_estimator.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class CostEstimatorTest : public ::testing::Test {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    a_a = node_a->get_column("a");
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a;
};

TEST_F(CostEstimatorTest, DiamondShape) {
  // Test that operations in a diamond-shaped LQP are only costed once

  const auto predicate_a = PredicateNode::make(less_than_equals_(a_a, 5), node_a);
  const auto predicate_b = PredicateNode::make(equals_(a_a, 5), predicate_a);
  const auto predicate_c = PredicateNode::make(equals_(a_a, 6), predicate_b);
  const auto union_node = UnionNode::make(UnionMode::Positions, predicate_b, predicate_c);

  using DummyCosts = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;

  const auto dummy_costs =
      DummyCosts{{node_a, 13.0f}, {predicate_a, 1.0f}, {predicate_b, 3.0f}, {predicate_c, 5.0f}, {union_node, 7.0f}};

  class MockCostEstimator : public AbstractCostEstimator {
   public:
    DummyCosts dummy_costs;

    explicit MockCostEstimator(const DummyCosts& dummy_costs) : dummy_costs(dummy_costs) {}

   protected:
    virtual Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const {
      return dummy_costs.at(node);
    }
  };

  EXPECT_EQ(MockCostEstimator{dummy_costs}.estimate_plan_cost(union_node), 29.0f);
}

}  // namespace opossum
