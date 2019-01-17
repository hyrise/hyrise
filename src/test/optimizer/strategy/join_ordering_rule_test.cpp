#include "gtest/gtest.h"

#include "cost_model/cost_model_logical.hpp"
#include "expression/arithmetic_expression.hpp"
#include "expression/between_expression.hpp"
#include "expression/binary_predicate_expression.hpp"
#include "expression/correlated_parameter_expression.hpp"
#include "expression/case_expression.hpp"
#include "expression/cast_expression.hpp"
#include "expression/evaluation/expression_evaluator.hpp"
#include "expression/evaluation/expression_result.hpp"
#include "expression/exists_expression.hpp"
#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "expression/extract_expression.hpp"
#include "expression/function_expression.hpp"
#include "expression/in_expression.hpp"
#include "expression/is_null_expression.hpp"
#include "expression/list_expression.hpp"
#include "expression/placeholder_expression.hpp"
#include "expression/lqp_select_expression.hpp"
#include "expression/lqp_column_expression.hpp"
#include "expression/pqp_column_expression.hpp"
#include "expression/pqp_select_expression.hpp"
#include "expression/unary_minus_expression.hpp"
#include "expression/value_expression.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "optimizer/strategy/join_ordering_rule.hpp"
#include "statistics/column_statistics.hpp"
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
    cost_estimator = std::make_shared<CostModelLogical>();
    rule = std::make_shared<JoinOrderingRule>(cost_estimator);

    // This test only makes sure THAT something gets reordered, not what the result of this reordering is - so the stats
    // are just dummies.
    const auto column_statistics = std::make_shared<ColumnStatistics<int32_t>>(0.0f, 10.0f, 1, 50);
    const auto table_statistics = std::make_shared<TableStatistics>(
        TableType::Data, 20, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

    node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "a");
    node_a->set_statistics(table_statistics);
    node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "b");
    node_b->set_statistics(table_statistics);
    node_c = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "c");
    node_c->set_statistics(table_statistics);
    node_d = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, "d");
    node_d->set_statistics(table_statistics);

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
