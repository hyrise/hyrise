#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_column_reference.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateReorderingTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    const auto table = load_table("resources/test_data/tbl/int_int_int.tbl");
    StorageManager::get().add_table("a", table);
    _rule = std::make_shared<PredicateReorderingRule>();

    std::vector<std::shared_ptr<const BaseColumnStatistics>> column_statistics(
        {std::make_shared<ColumnStatistics<int32_t>>(0.0f, 20, 10, 100),
         std::make_shared<ColumnStatistics<int32_t>>(0.0f, 5, 50, 60),
         std::make_shared<ColumnStatistics<int32_t>>(0.0f, 2, 110, 1100)});

    auto table_statistics = std::make_shared<TableStatistics>(TableType::Data, 100, column_statistics);
    // Assumes 50% deleted rows
    table_statistics->increase_invalid_row_count(50);

    node = StoredTableNode::make("a");
    table->set_table_statistics(table_statistics);

    a = LQPColumnReference{node, ColumnID{0}};
    b = LQPColumnReference{node, ColumnID{1}};
    c = LQPColumnReference{node, ColumnID{2}};
  }

  std::shared_ptr<StoredTableNode> node;
  LQPColumnReference a, b, c;
  std::shared_ptr<PredicateReorderingRule> _rule;
};

TEST_F(PredicateReorderingTest, SimpleReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a, 50),
    PredicateNode::make(greater_than_(a, 10),
      node));
  const auto expected_lqp =
  PredicateNode::make(greater_than_(a, 10),
    PredicateNode::make(greater_than_(a, 50),
      node));
  // clang-format on

  const auto reordered_input_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

TEST_F(PredicateReorderingTest, MoreComplexReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a, 99),
    PredicateNode::make(greater_than_(b, 55),
      PredicateNode::make(greater_than_(c, 100),
        node)));
  const auto expected_lqp =
  PredicateNode::make(greater_than_(c, 100),
    PredicateNode::make(greater_than_(b, 55),
      PredicateNode::make(greater_than_(a, 99),
        node)));
  // clang-format on

  const auto reordered_input_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a, 42),
    PredicateNode::make(greater_than_(b, 50),
      PredicateNode::make(greater_than_(b, 40),
        ProjectionNode::make(expression_vector(a, b, c),
          PredicateNode::make(greater_than_equals_(a, 90),
            PredicateNode::make(less_than_(c, 500),
              node))))));


  const auto expected_optimized_lqp =
  PredicateNode::make(greater_than_(b, 40),
    PredicateNode::make(greater_than_(b, 50),
      PredicateNode::make(equals_(a, 42),
        ProjectionNode::make(expression_vector(a, b, c),
          PredicateNode::make(less_than_(c, 500),
            PredicateNode::make(greater_than_equals_(a, 90),
              node))))));
  // clang-format on

  const auto reordered_input_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_optimized_lqp);
}

TEST_F(PredicateReorderingTest, SameOrderingForStoredTable) {
  std::shared_ptr<Table> table_a = load_table("resources/test_data/tbl/int_float4.tbl", 2);
  StorageManager::get().add_table("table_a", std::move(table_a));

  auto stored_table_node = StoredTableNode::make("table_a");

  // Setup first LQP
  // predicate_node_1 -> predicate_node_0 -> stored_table_node
  auto predicate_node_0 = PredicateNode::make(less_than_(LQPColumnReference{stored_table_node, ColumnID{0}}, 20));
  predicate_node_0->set_left_input(stored_table_node);

  auto predicate_node_1 = PredicateNode::make(less_than_(LQPColumnReference{stored_table_node, ColumnID{0}}, 40));
  predicate_node_1->set_left_input(predicate_node_0);

  predicate_node_1->get_statistics();

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_1);

  // Setup second LQP
  // predicate_node_3 -> predicate_node_2 -> stored_table_node
  auto predicate_node_2 = PredicateNode::make(less_than_(LQPColumnReference{stored_table_node, ColumnID{0}}, 40));
  predicate_node_2->set_left_input(stored_table_node);

  auto predicate_node_3 = PredicateNode::make(less_than_(LQPColumnReference{stored_table_node, ColumnID{0}}, 20));
  predicate_node_3->set_left_input(predicate_node_2);

  auto reordered_1 = StrategyBaseTest::apply_rule(_rule, predicate_node_3);

  EXPECT_EQ(reordered, predicate_node_1);
  EXPECT_EQ(reordered->left_input(), predicate_node_0);
  EXPECT_EQ(reordered_1, predicate_node_2);
  EXPECT_EQ(reordered_1->left_input(), predicate_node_3);
}

TEST_F(PredicateReorderingTest, PredicatesAsRightInput) {
  /**
   * Check that Reordering predicates works if a predicate chain is both on the left and right side of a node.
   * This is particularly interesting because the PredicateReorderingRule needs to re-attach the ordered chain of
   * predicates to the output (the cross node in this case). This test checks whether the attachment happens as the
   * correct input.
   *
   *             _______Cross________
   *            /                    \
   *  Predicate_0(a > 80)     Predicate_2(a > 90)
   *           |                     |
   *  Predicate_1(a > 60)     Predicate_3(a > 50)
   *           |                     |
   *        Table_0           Predicate_4(a > 30)
   *                                 |
   *                               Table_1
   */

  /**
   * The mocked table has one column of int32_ts with the value range 0..100
   */
  auto column_statistics = std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, 100.0f, 0.0f, 100.0f);
  auto table_statistics = std::make_shared<TableStatistics>(
      TableType::Data, 100, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

  auto table_0 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
  table_0->set_statistics(table_statistics);
  auto table_1 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
  table_1->set_statistics(table_statistics);
  auto cross_node = JoinNode::make(JoinMode::Cross);
  auto predicate_0 = PredicateNode::make(greater_than_(LQPColumnReference{table_0, ColumnID{0}}, 80));
  auto predicate_1 = PredicateNode::make(greater_than_(LQPColumnReference{table_0, ColumnID{0}}, 60));
  auto predicate_2 = PredicateNode::make(greater_than_(LQPColumnReference{table_1, ColumnID{0}}, 90));
  auto predicate_3 = PredicateNode::make(greater_than_(LQPColumnReference{table_1, ColumnID{0}}, 50));
  auto predicate_4 = PredicateNode::make(greater_than_(LQPColumnReference{table_1, ColumnID{0}}, 30));

  predicate_1->set_left_input(table_0);
  predicate_0->set_left_input(predicate_1);
  predicate_4->set_left_input(table_1);
  predicate_3->set_left_input(predicate_4);
  predicate_2->set_left_input(predicate_3);
  cross_node->set_left_input(predicate_0);
  cross_node->set_right_input(predicate_2);

  const auto reordered = StrategyBaseTest::apply_rule(_rule, cross_node);

  EXPECT_EQ(reordered, cross_node);
  EXPECT_EQ(reordered->left_input(), predicate_1);
  EXPECT_EQ(reordered->left_input()->left_input(), predicate_0);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input(), table_0);
  EXPECT_EQ(reordered->right_input(), predicate_4);
  EXPECT_EQ(reordered->right_input()->left_input(), predicate_3);
  EXPECT_EQ(reordered->right_input()->left_input()->left_input(), predicate_2);
}

TEST_F(PredicateReorderingTest, PredicatesWithMultipleOutputs) {
  /**
   * If a PredicateNode has multiple outputs, it should not be considered for reordering
   */
  /**
   *      _____Union___
   *    /             /
   * Predicate_a     /
   *    \           /
   *     Predicate_b
   *         |
   *       Table
   *
   * predicate_a should come before predicate_b - but since Predicate_b has two outputs, it can't be reordered
   */

  /**
   * The mocked table has one column of int32_ts with the value range 0..100
   */
  auto column_statistics = std::make_shared<ColumnStatistics<int32_t>>(ColumnID{0}, 100.0f, 0.0f, 100.0f);
  auto table_statistics = std::make_shared<TableStatistics>(
      TableType::Data, 100, std::vector<std::shared_ptr<const BaseColumnStatistics>>{column_statistics});

  auto table_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}});
  table_node->set_statistics(table_statistics);
  auto union_node = UnionNode::make(UnionMode::Positions);
  auto predicate_a_node = PredicateNode::make(greater_than_(LQPColumnReference{table_node, ColumnID{0}}, 90));
  auto predicate_b_node = PredicateNode::make(greater_than_(LQPColumnReference{table_node, ColumnID{0}}, 10));

  union_node->set_left_input(predicate_a_node);
  union_node->set_right_input(predicate_b_node);
  predicate_a_node->set_left_input(predicate_b_node);
  predicate_b_node->set_left_input(table_node);

  const auto reordered = StrategyBaseTest::apply_rule(_rule, union_node);

  EXPECT_EQ(reordered, union_node);
  EXPECT_EQ(reordered->left_input(), predicate_a_node);
  EXPECT_EQ(reordered->right_input(), predicate_b_node);
  EXPECT_EQ(predicate_a_node->left_input(), predicate_b_node);
  EXPECT_EQ(predicate_b_node->left_input(), table_node);
}

TEST_F(PredicateReorderingTest, SimpleValidateReorderingTest) {
  // clang-format off
  const auto input_lqp =
    PredicateNode::make(greater_than_(a, 60),
      ValidateNode::make(node));

  const auto expected_lqp =
    ValidateNode::make(
      PredicateNode::make(greater_than_(a, 60),
        node));
  // clang-format on

  const auto reordered_input_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

TEST_F(PredicateReorderingTest, SecondValidateReorderingTest) {
  // clang-format off
  const auto input_lqp =
    PredicateNode::make(greater_than_(a, 30),
      ValidateNode::make(node));

  const auto expected_lqp =
    PredicateNode::make(greater_than_(a, 30),
      ValidateNode::make(node));
  // clang-format on

  const auto reordered_input_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(reordered_input_lqp, expected_lqp)
}

}  // namespace opossum
