#include <unordered_map>

#include "base_test.hpp"

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/union_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace {

using namespace opossum;  // NOLINT

using MockCosts = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;

class MockCostEstimator : public AbstractCostEstimator {
 public:
  MockCosts mock_costs;

  explicit MockCostEstimator(const MockCosts& mock_costs) : AbstractCostEstimator(nullptr), mock_costs(mock_costs) {}

  std::shared_ptr<AbstractCostEstimator> new_instance() const override { Fail("Shouldn't be called"); }

  Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const override { return mock_costs.at(node); }
};

}  // namespace

namespace opossum {

class AbstractCostEstimatorTest : public BaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    a_a = node_a->get_column("a");
  }

  std::shared_ptr<MockNode> node_a;
  LQPColumnReference a_a;
};

TEST_F(AbstractCostEstimatorTest, DiamondShape) {
  // Test that operations in a diamond-shaped LQP are only costed once

  const auto predicate_a = PredicateNode::make(less_than_equals_(a_a, 5), node_a);
  const auto predicate_b = PredicateNode::make(equals_(a_a, 5), predicate_a);
  const auto predicate_c = PredicateNode::make(equals_(a_a, 6), predicate_b);
  const auto union_node = UnionNode::make(UnionMode::Positions, predicate_b, predicate_c);

  const auto mock_costs =
      MockCosts{{node_a, 13.0f}, {predicate_a, 1.0f}, {predicate_b, 3.0f}, {predicate_c, 5.0f}, {union_node, 7.0f}};

  EXPECT_EQ(MockCostEstimator{mock_costs}.estimate_plan_cost(union_node), 29.0f);
}

TEST_F(AbstractCostEstimatorTest, PlanCostCache) {
  const auto predicate_a = PredicateNode::make(less_than_equals_(a_a, 5), node_a);
  const auto predicate_b = PredicateNode::make(equals_(a_a, 5), predicate_a);
  const auto predicate_c = PredicateNode::make(equals_(a_a, 6), predicate_b);

  const auto mock_costs = MockCosts{{node_a, 13.0f}, {predicate_a, 1.0f}, {predicate_b, 3.0f}, {predicate_c, 5.0f}};

  MockCostEstimator cost_estimator{mock_costs};
  cost_estimator.cost_estimation_by_lqp_cache.emplace();
  cost_estimator.cost_estimation_by_lqp_cache->emplace(node_a, 1000.0f);  // Should not be retrieved from cache
  cost_estimator.cost_estimation_by_lqp_cache->emplace(predicate_b, 77.0f);

  // This should sum the cost of predicate_c with the cached cost for its input plan
  EXPECT_EQ(cost_estimator.estimate_plan_cost(predicate_c), 82.0f);

  // Check that there is a new entry (for predicate_c) in the cache
  EXPECT_EQ(cost_estimator.cost_estimation_by_lqp_cache->count(predicate_c), 1u);
  EXPECT_EQ(cost_estimator.cost_estimation_by_lqp_cache->at(predicate_c), 82.0f);
}

TEST_F(AbstractCostEstimatorTest, PlanCostCacheDiamondShape) {
  const auto predicate_a = PredicateNode::make(less_than_equals_(a_a, 5), node_a);
  const auto predicate_b = PredicateNode::make(equals_(a_a, 5), predicate_a);
  const auto predicate_c = PredicateNode::make(equals_(a_a, 6), predicate_b);
  const auto predicate_d = PredicateNode::make(equals_(a_a, 7), predicate_b);
  const auto union_node = UnionNode::make(UnionMode::Positions, predicate_d, predicate_c);

  const auto mock_costs = MockCosts{{node_a, 13.0f},     {predicate_a, 1.0f},  {predicate_b, 3.0f},
                                    {predicate_c, 5.0f}, {predicate_d, 99.0f}, {union_node, 9.0f}};

  MockCostEstimator cost_estimator{mock_costs};

  cost_estimator.cost_estimation_by_lqp_cache.emplace();
  cost_estimator.cost_estimation_by_lqp_cache->emplace(node_a, 1000.0f);  // Should not be retrieved from cache
  cost_estimator.cost_estimation_by_lqp_cache->emplace(predicate_c, 77.0f);
  cost_estimator.cost_estimation_by_lqp_cache->emplace(predicate_d, 115.0f);
  cost_estimator.cost_estimation_by_lqp_cache->emplace(predicate_b, 999.0f);

  // This should sum the cost of predicate_d, union_node with the cached cost for predicate_c
  EXPECT_EQ(cost_estimator.estimate_plan_cost(union_node), 115.0f + 5.0f + 9.0f);
  EXPECT_EQ(cost_estimator.estimate_plan_cost(predicate_d), 115.0f);
  EXPECT_EQ(cost_estimator.estimate_plan_cost(predicate_c), 77.0f);
}

}  // namespace opossum
