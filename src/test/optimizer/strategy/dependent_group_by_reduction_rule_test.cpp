#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "optimizer/strategy/dependent_group_by_reduction_rule.hpp"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DependentGroupByReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    TableColumnDefinitions column_definitions{{"column0", DataType::Int, false}, {"column1", DataType::Int, false}, {"column2", DataType::Int, false}};

    table_a = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_a->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::Yes);
    storage_manager.add_table("table_a", table_a);
    stored_table_node_a = StoredTableNode::make("table_a");
    column_a_0 = stored_table_node_a->get_column("column0");
    column_a_1 = stored_table_node_a->get_column("column1");
    column_a_2 = stored_table_node_a->get_column("column2");

    table_b = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_b->add_soft_unique_constraint({ColumnID{0}, ColumnID{1}}, IsPrimaryKey::Yes);
    storage_manager.add_table("table_b", table_b);
    stored_table_node_b = StoredTableNode::make("table_b");
    column_b_0 = stored_table_node_b->get_column("column0");
    column_b_1 = stored_table_node_b->get_column("column1");
    column_b_2 = stored_table_node_b->get_column("column2");

    table_c = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_c->add_soft_unique_constraint({ColumnID{0}, ColumnID{2}}, IsPrimaryKey::Yes);
    storage_manager.add_table("table_c", table_c);
    stored_table_node_c = StoredTableNode::make("table_c");
    column_c_0 = stored_table_node_c->get_column("column0");
    column_c_1 = stored_table_node_c->get_column("column1");
    column_c_2 = stored_table_node_c->get_column("column2");

    table_d = std::make_shared<Table>(TableColumnDefinitions{{"column0", DataType::Int, false}}, TableType::Data, 2, UseMvcc::Yes);
    storage_manager.add_table("table_d", table_d);

    rule = std::make_shared<DependentGroupByReductionRule>();
  }

  std::shared_ptr<DependentGroupByReductionRule> rule;

  std::shared_ptr<Table> table_a, table_b, table_c, table_d;
  std::shared_ptr<StoredTableNode> stored_table_node_a, stored_table_node_b, stored_table_node_c;
  LQPColumnReference column_a_0, column_a_1, column_a_2;
  LQPColumnReference column_b_0, column_b_1, column_b_2;
  LQPColumnReference column_c_0, column_c_1, column_c_2;
};

// Test simple cases
TEST_F(DependentGroupByReductionRuleTest, SimpleCases) {
  // Early out for LQP without any aggregates
  // clang-format off
  {
    auto lqp =
    PredicateNode::make(equals_(column_a_0, 17), stored_table_node_a);

    const auto unmodified_lqp = apply_rule(rule, lqp);
    EXPECT_EQ(lqp, unmodified_lqp);
  }

  // Early out for LQP where table does not have a primary key
  {
    auto stored_table_node_d = StoredTableNode::make("table_d");
    auto column_d_0 = stored_table_node_d->get_column("column0");

    auto lqp =
    PredicateNode::make(equals_(column_d_0, 17), stored_table_node_d);

    const auto unmodified_lqp = apply_rule(rule, lqp);
    EXPECT_EQ(lqp, unmodified_lqp);
  }
  // clang-format on
}

// Test that a removable column is removed when the single primary key column is present.
TEST_F(DependentGroupByReductionRuleTest, SingleKeyReduction) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_a_0, column_a_1),
                      expression_vector(sum_(column_a_0), sum_(column_a_1),
                                        sum_(column_a_2)),
                      stored_table_node_a);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_a_0),
                      expression_vector(sum_(column_a_0), sum_(column_a_1),
                                        sum_(column_a_2)),
                      stored_table_node_a);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that a non-primary-key column is not removed if the full primary key is not present in the group by list.
TEST_F(DependentGroupByReductionRuleTest, IncompleteKey) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_2),
                      expression_vector(sum_(column_b_0), sum_(column_b_1),
                                        sum_(column_b_2)),
                      stored_table_node_b);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_2),
                      expression_vector(sum_(column_b_0), sum_(column_b_1),
                                        sum_(column_b_2)),
                      stored_table_node_b);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that a group by with the full (multi-column) primary key is not altered.
TEST_F(DependentGroupByReductionRuleTest, FullKeyGroupBy) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_1),
                      expression_vector(sum_(column_b_0), sum_(column_b_1),
                                        sum_(column_b_2)),
                      stored_table_node_b);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_1),
                      expression_vector(sum_(column_b_0), sum_(column_b_1),
                                        sum_(column_b_2)),
                      stored_table_node_b);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test adaption of multi-column and but inconsecutive column order primary key columns (table_c with {0,2})
TEST_F(DependentGroupByReductionRuleTest, FullInconsecutiveKeyGroupBy) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(column_c_0, column_c_1, column_c_2),
    AggregateNode::make(expression_vector(column_c_0, column_c_1, column_c_2),
                        expression_vector(sum_(column_c_0), sum_(column_c_2)),
                        stored_table_node_c));

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_c_0, any_(column_c_1), column_c_2),
    AggregateNode::make(expression_vector(column_c_0, column_c_2),
                        expression_vector(sum_(column_c_0), sum_(column_c_2), any_(column_c_1)),
                        stored_table_node_c));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that a column that can be removed from the group by list and is not accessed later is removed completely.
TEST_F(DependentGroupByReductionRuleTest, UnnecessaryGroupByColumn) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_a_0, column_a_1),
                      expression_vector(sum_(column_a_0)),
                      stored_table_node_a);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_a_0),
                      expression_vector(sum_(column_a_0)),
                      stored_table_node_a);
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that a column can be removed from the group by list, but is later accessed and thus needs to be ANY()'d.
TEST_F(DependentGroupByReductionRuleTest, RemovedAndLaterAccessedGroupByColumn) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(column_a_0, column_a_1),
    AggregateNode::make(expression_vector(column_a_0, column_a_1),
                        expression_vector(sum_(column_a_0)),
                        stored_table_node_a));

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_a_0, any_(column_a_1)),
    AggregateNode::make(expression_vector(column_a_0),
                        expression_vector(sum_(column_a_0), any_(column_a_1)),
                        stored_table_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test whether we remove the correct columns when a table is present multiple times (e.g., after self join).
TEST_F(DependentGroupByReductionRuleTest, SelfJoinSingleKeyPrimaryKey) {
  const auto stored_table_node_a2 = StoredTableNode::make("table_a");
  const auto column_a2_0 = stored_table_node_a->get_column("column0");
  const auto column_a2_1 = stored_table_node_a->get_column("column1");
  const auto column_a2_2 = stored_table_node_a->get_column("column2");

  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5)),
    AggregateNode::make(expression_vector(column_a_0, column_a_1, column_a2_0, column_a2_1),
                        expression_vector(sum_(column_a_0)),
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_a2_0),
                     stored_table_node_a,
                     stored_table_node_a2)));

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(any_(column_a_1), 5)),
    AggregateNode::make(expression_vector(column_a_0, column_a2_0),
                        expression_vector(sum_(column_a_0), any_(column_a_1)),
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_a2_0),
                     stored_table_node_a,
                     stored_table_node_a2)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test similar to previous test, but one join side has full grouping of primary key, the other not.
TEST_F(DependentGroupByReductionRuleTest, SelfJoinMultiKeyPrimaryKey) {
  const auto stored_table_node_b2 = StoredTableNode::make("table_b");
  const auto column_b2_0 = stored_table_node_b->get_column("column0");
  const auto column_b2_1 = stored_table_node_b->get_column("column1");
  const auto column_b2_2 = stored_table_node_b->get_column("column2");

  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(column_b_0, 5), add_(any_(column_b_2), 5)),
    AggregateNode::make(expression_vector(column_b_0, column_b_2, column_b2_0, column_b2_1),
                        expression_vector(sum_(column_b_0)),
      JoinNode::make(JoinMode::Inner, equals_(column_b_0, column_b2_0),
                     stored_table_node_b,
                     stored_table_node_b2)));

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(column_b_0, 5), add_(any_(column_b_2), 5)),
    AggregateNode::make(expression_vector(column_b_0, column_b2_0, column_b2_1),
                        expression_vector(sum_(column_b_0), any_(column_b_2)),
      JoinNode::make(JoinMode::Inner, equals_(column_b_0, column_b2_0),
                     stored_table_node_b,
                     stored_table_node_b2)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
