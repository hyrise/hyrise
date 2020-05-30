#include <memory>
#include <optional>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/predicate_reordering_rule.hpp"
#include "statistics/table_statistics.hpp"

#include "utils/assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicateReorderingTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    _rule = std::make_shared<PredicateReorderingRule>();

    node = create_mock_node_with_statistics(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, 100,
        {GenericHistogram<int32_t>::with_single_bin(10, 100, 100, 20),
         GenericHistogram<int32_t>::with_single_bin(50, 60, 100, 5),
         GenericHistogram<int32_t>::with_single_bin(110, 1100, 100, 2)});

    a = node->get_column("a");
    b = node->get_column("b");
    c = node->get_column("c");
  }

  std::shared_ptr<MockNode> node;
  std::shared_ptr<LQPColumnExpression> a, b, c;
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

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
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

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingTest, ComplexReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(a, 95),
    PredicateNode::make(greater_than_(b, 55),
      PredicateNode::make(greater_than_(b, 40),
        ProjectionNode::make(expression_vector(a, b, c),
          PredicateNode::make(greater_than_equals_(a, 90),
            PredicateNode::make(less_than_(c, 500),
              node))))));


  const auto expected_lqp =
  PredicateNode::make(greater_than_(b, 40),
    PredicateNode::make(greater_than_(b, 55),
      PredicateNode::make(equals_(a, 95),
        ProjectionNode::make(expression_vector(a, b, c),
          PredicateNode::make(less_than_(c, 500),
            PredicateNode::make(greater_than_equals_(a, 90),
              node))))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingTest, SameOrderingForStoredTable) {
  std::shared_ptr<Table> table_a = load_table("resources/test_data/tbl/int_float4.tbl", 2);
  Hyrise::get().storage_manager.add_table("table_a", std::move(table_a));

  auto stored_table_node = StoredTableNode::make("table_a");

  {
    // clang-format off
    const auto input_lqp =
    PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 40),
      PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 20),
        stored_table_node));
    // clang-format on
    const auto expected_lqp = input_lqp->deep_copy();

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    // clang-format off
    const auto input_lqp =
    PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 20),
      PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 400),
        stored_table_node));

    const auto expected_lqp =
    PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 400),
      PredicateNode::make(less_than_(lqp_column_(stored_table_node, ColumnID{0}), 20),
        stored_table_node));
    // clang-format on

    const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
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
     *  Predicate(a > 80)     Predicate(a > 90)
     *           |                     |
     *  Predicate(a > 60)     Predicate(a > 50)
     *           |                     |
     *        Table_0         Predicate(a > 30)
     *                                 |
     *                              Table_1
     */

  /**
     * The mocked table has one column of int32_ts with the value range 0..100
     */
  auto table_0 = create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, 100.0f,
                                                  {GenericHistogram<int32_t>::with_single_bin(0, 100, 100.0f, 100.0f)});
  auto table_1 = create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, 100.0f,
                                                  {GenericHistogram<int32_t>::with_single_bin(0, 100, 100.0f, 100.0f)});

  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(greater_than_(lqp_column_(table_0, ColumnID{0}), 80),
      PredicateNode::make(greater_than_(lqp_column_(table_0, ColumnID{0}), 60),
        table_0)),
    PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 90),
      PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 50),
        PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 30),
          table_1))));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Cross,
    PredicateNode::make(greater_than_(lqp_column_(table_0, ColumnID{0}), 60),
      PredicateNode::make(greater_than_(lqp_column_(table_0, ColumnID{0}), 80),
        table_0)),
    PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 30),
      PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 50),
        PredicateNode::make(greater_than_(lqp_column_(table_1, ColumnID{0}), 90),
          table_1))));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingTest, PredicatesWithMultipleOutputs) {
  /**
     * If a PredicateNode has multiple outputs, it should only be considered for reordering with lower predicates.
     */
  /**
     *        ____Union___
     *       /            \
     * Predicate(a > 90)  |
     *       \            /
     *      Predicate(a > 10)
     *             |
     *      Predicate(a > 5)
     *             |
     *           Table
     *
     * Predicate_a has a lower selectivity than Predicate_b - but since Predicate_b has two outputs, Predicate_a cannot
     * be reordered since it does not belong to the predicate chain (Predicate_b and Predicate_c). However, Predicate_b
     * and Predicate_c can be reordered inside their chain.
     */

  /**
     * The mocked table has one column of int32_ts with the value range 0..100
     */
  auto table_node =
      create_mock_node_with_statistics(MockNode::ColumnDefinitions{{DataType::Int, "a"}}, 100.0f,
                                       {GenericHistogram<int32_t>::with_single_bin(0, 100, 100.0f, 100.0f)});

  // clang-format off
  const auto sub_lqp =
  PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 10),
    PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 5),
      table_node));

  const auto input_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 90),
      sub_lqp),
    sub_lqp);

  const auto expected_sub_lqp =
  PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 5),
    PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 10),
      table_node));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(greater_than_(lqp_column_(table_node, ColumnID{0}), 90),
      expected_sub_lqp),
    expected_sub_lqp);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicateReorderingTest, SimpleValidateReorderingTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(a, 60),
    ValidateNode::make(
      node));

  const auto expected_lqp =
  ValidateNode::make(
    PredicateNode::make(greater_than_(a, 60),
      node));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
