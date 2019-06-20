#include <unordered_map>

#include "gtest/gtest.h"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "statistics/cardinality_estimator.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

// TODO(anyone): disabled due to Moritz's histogram commit. Might be unnecessary as I assume diamonds are already tested "somewhere".

// class CostEstimatorTest : public ::testing::Test {
//  public:
//   void SetUp() override {
//     node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
//     a_a = node_a->get_column("a");
//   }

//   std::shared_ptr<MockNode> node_a;
//   LQPColumnReference a_a;
// };

// TEST_F(CostEstimatorTest, DiamondShape) {
//   // Test that operations in a diamond-shaped LQP are only costed once

//   const auto predicate_a = PredicateNode::make(less_than_equals_(a_a, 5), node_a);
//   const auto predicate_b = PredicateNode::make(equals_(a_a, 5), predicate_a);
//   const auto predicate_c = PredicateNode::make(equals_(a_a, 6), predicate_b);
//   const auto union_node = UnionNode::make(UnionMode::Positions, predicate_b, predicate_c);

//   using DummyCosts = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;

//   const auto dummy_costs =
//       DummyCosts{{node_a, 13.0f}, {predicate_a, 1.0f}, {predicate_b, 3.0f}, {predicate_c, 5.0f}, {union_node, 7.0f}};

//   class MockCostEstimator : public AbstractCostEstimator {
//    public:
//     DummyCosts dummy_costs;

//     std::shared_ptr<AbstractCostEstimator> new_instance() const override {
//       return std::make_shared<MockCostEstimator>(cardinality_estimator->new_instance());
//     }

//    protected:
//     Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override {
//       return dummy_costs.at(node);
//     }
//   };

//   auto mock_estimator = std::make_shared<MockCostEstimator>(std::make_shared<CardinalityEstimator>());

//   EXPECT_EQ(mock_estimator->estimate_plan_cost(union_node), 29.0f);
// }

}  // namespace opossum
