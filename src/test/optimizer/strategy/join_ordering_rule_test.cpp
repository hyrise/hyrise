#include "gtest/gtest.h"

#include "cost_model/cost_model_logical.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "statistics/chunk_statistics/histograms/single_bin_histogram.hpp"
#include "statistics/chunk_statistics2.hpp"
#include "statistics/segment_statistics2.hpp"
#include "statistics/table_statistics2.hpp"

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
    const auto segment_histogram = std::make_shared<SingleBinHistogram<int32_t>>(1, 50, 20, 10);

    const auto segment_statistics = std::make_shared<SegmentStatistics2<int32_t>>();
    segment_statistics->set_statistics_object(segment_histogram);

    const auto chunk_statistics = std::make_shared<ChunkStatistics2>(10);
    chunk_statistics->segment_statistics.emplace_back(segment_statistics);

    const auto table_statistics = std::make_shared<TableStatistics2>();
    table_statistics->chunk_statistics_sets.resize(1);
    table_statistics->chunk_statistics_sets.front().emplace_back(chunk_statistics);

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    node_a->set_table_statistics2(table_statistics);
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    node_b->set_table_statistics2(table_statistics);
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    node_c->set_table_statistics2(table_statistics);
    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "d");
    node_d->set_table_statistics2(table_statistics);

    a_a = node_a->get_column("a");
    b_a = node_b->get_column("a");
    c_a = node_c->get_column("a");
    d_a = node_d->get_column("a");
  }

  std::shared_ptr<MockNode> node_a, node_b, node_c, node_d;
  LQPColumnReference a_a, b_a, c_a, d_a;
  std::shared_ptr<AbstractCostEstimator> cost_estimator;
  std::shared_ptr<JoinOrderingRule> rule;
};

TEST_F(JoinOrderingRuleTest, MultipleJoinGraphs) {
  // Test that the JoinOrderingRule works when there are multiple parts in the plan that need isolated optimization
  // e.g., when there is a barrier in the form of an outer join

  // clang-format off
  const auto input_lqp =
  AggregateNode::make(expression_vector(a_a), expression_vector(),
    PredicateNode::make(equals_(a_a, b_a),
      JoinNode::make(JoinMode::Cross,
        node_a,
        JoinNode::make(JoinMode::Left, equals_(b_a, d_a),
          node_b,
          PredicateNode::make(equals_(d_a, c_a),
            JoinNode::make(JoinMode::Cross,
              node_d,
              node_c))))));

  const auto expected_lqp =
  AggregateNode::make(expression_vector(a_a), expression_vector(),
    JoinNode::make(JoinMode::Inner, equals_(a_a, b_a),
      node_a,
      JoinNode::make(JoinMode::Left, equals_(b_a, d_a),
        node_b,
        JoinNode::make(JoinMode::Inner, equals_(d_a, c_a),
          node_d,
          node_c))));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
