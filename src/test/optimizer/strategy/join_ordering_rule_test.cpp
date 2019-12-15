#include "gtest/gtest.h"

#include "cost_estimation/cost_estimator_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "statistics/attribute_statistics.hpp"
#include "statistics/table_statistics.hpp"

#include "strategy_base_test.hpp"

/**
 * We can't actually test much about the JoinOrderingRule, since it is highly dependent on the underlying algorithms
 * which are separately tested.
 */

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class JoinOrderingRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    rule = std::make_shared<JoinOrderingRule>();

    // This test only makes sure THAT something gets reordered, not what the result of this reordering is - so the stats
    // are just dummies.
    const auto histogram = GenericHistogram<int32_t>::with_single_bin(1, 50, 20, 10);

    node_a = create_mock_node_with_statistics({{DataType::Int, "a"}}, 20, {histogram});
    node_b = create_mock_node_with_statistics({{DataType::Int, "b"}}, 20, {histogram});
    node_c = create_mock_node_with_statistics({{DataType::Int, "c"}}, 20, {histogram});
    node_d = create_mock_node_with_statistics({{DataType::Int, "d"}}, 20, {histogram});

    a_a = node_a->get_column("a");
    b_b = node_b->get_column("b");
    c_c = node_c->get_column("c");
    d_d = node_d->get_column("d");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  LQPColumnReference a_a, b_b, c_c, d_d;
  std::shared_ptr<AbstractCostEstimator> cost_estimator;
  std::shared_ptr<JoinOrderingRule> rule;
};

TEST_F(JoinOrderingRuleTest, MultipleJoinGraphs) {
  // Test that the JoinOrderingRule works when there are multiple parts in the plan that need isolated optimization
  // e.g., when there is a barrier in the form of an outer join

  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_a), expression_vector(),
    PredicateNode::make(equals_(a_a, b_b),
      JoinNode::make(JoinMode::Cross,
        node_a,
        JoinNode::make(JoinMode::Left, equals_(b_b, d_d),
          node_b,
          PredicateNode::make(equals_(d_d, c_c),
            JoinNode::make(JoinMode::Cross,
              node_d,
              node_c))))));

  const auto actual_lqp = apply_rule(rule, input_lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(a_a), expression_vector(),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_b),
      JoinNode::make(JoinMode::Left, equals_(b_b, d_d),
        node_b,
        JoinNode::make(JoinMode::Inner, equals_(d_d, c_c),
          node_d,
          node_c)),
      node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
