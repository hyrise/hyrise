#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "expression/expression_functional.hpp"
#include "expression/lqp_column_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/join_predicate_ordering_rule.hpp"
#include "utils/load_table.hpp"

namespace opossum {

class JoinPredicateOrderingRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    _rule = std::make_shared<JoinPredicateOrderingRule>();

    node_a = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}, {DataType::Int, "z"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(0, 40, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(0, 40, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(0, 40, 100, 5)});
    node_a->name = "a";
    a_x = node_a->get_column("x");
    a_y = node_a->get_column("y");
    a_z = node_a->get_column("z");

    node_b = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Int, "y"}, {DataType::Int, "z"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(0, 40, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(30, 70, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(10, 50, 100, 5)});
    node_b->name = "b";
    b_x = node_b->get_column("x");
    b_y = node_b->get_column("y");
    b_z = node_b->get_column("z");
  }

  std::shared_ptr<JoinPredicateOrderingRule> _rule;
  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a_x, a_y, a_z, b_x, b_y, b_z;
};

TEST_F(JoinPredicateOrderingRuleTest, BasicJoinPredicateOrdering) {
  const auto input_join_predicates = expression_vector(equals_(a_x, b_x), equals_(a_y, b_y), equals_(a_z, b_z));
  const auto input_lqp = JoinNode::make(JoinMode::Inner, input_join_predicates, node_a, node_b);

  // We assume equals_(a_y, b_y) > equals_(a_z, b_z) > equals_(a_x, b_x) wrt. selectivity. This might change when
  //  the cardinality estimation changes. In that case the histograms in SetUp should be updated.
  const auto expected_join_predicates = expression_vector(equals_(a_y, b_y), equals_(a_z, b_z), equals_(a_x, b_x));
  const auto expected_lqp = JoinNode::make(JoinMode::Inner, expected_join_predicates, node_a, node_b);

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
