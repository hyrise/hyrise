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
#include "optimizer/strategy/column_pruning_rule.hpp"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ColumnPruningRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    node_a = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "a");
    node_b = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}, {DataType::Int, "w"}}, "b");

    a = node_a->get_column("a");
    b = node_a->get_column("b");
    c = node_a->get_column("c");
    u = node_b->get_column("u");
    v = node_b->get_column("v");
    w = node_b->get_column("w");

    rule = std::make_shared<ColumnPruningRule>();
  }

  const std::shared_ptr<MockNode> pruned(const std::shared_ptr<MockNode> node,
                                         const std::vector<ColumnID>& column_ids) {
    const auto pruned_node = std::static_pointer_cast<MockNode>(node->deep_copy());
    pruned_node->set_pruned_column_ids(column_ids);
    return pruned_node;
  }

  std::shared_ptr<ColumnPruningRule> rule;
  std::shared_ptr<MockNode> node_a, node_b;
  LQPColumnReference a, b, c, u, v, w;
};

TEST_F(ColumnPruningRuleTest, NoUnion) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(mul_(a, u), 5)),
    PredicateNode::make(greater_than_(5, c),
      JoinNode::make(JoinMode::Inner, greater_than_(v, a),
        node_a,
        SortNode::make(expression_vector(w), std::vector<OrderByMode>{OrderByMode::Ascending},  // NOLINT
          node_b))));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_c = pruned_node_a->get_column("c");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(mul_(pruned_a, u), 5)),
    PredicateNode::make(greater_than_(5, pruned_c),
      JoinNode::make(JoinMode::Inner, greater_than_(v, pruned_a),
        pruned_node_a,
        SortNode::make(expression_vector(w), std::vector<OrderByMode>{OrderByMode::Ascending},  // NOLINT
          node_b))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, WithUnion) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(greater_than_(a, 5),
        node_a),
      PredicateNode::make(greater_than_(b, 5),
        node_a)));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(pruned_a),
    UnionNode::make(UnionMode::Positions,
      PredicateNode::make(greater_than_(pruned_a, 5),
        pruned_node_a),
      PredicateNode::make(greater_than_(pruned_b, 5),
        pruned_node_a)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, WithMultipleProjections) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(a),
    PredicateNode::make(greater_than_(mul_(a, b), 5),
      ProjectionNode::make(expression_vector(a, b, mul_(a, b), c),
        PredicateNode::make(greater_than_(mul_(a, 2), 5),
          ProjectionNode::make(expression_vector(a, b, mul_(a, 2), c),
            node_a)))));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(pruned_a),
    PredicateNode::make(greater_than_(mul_(pruned_a, pruned_b), 5),
      ProjectionNode::make(expression_vector(pruned_a, mul_(pruned_a, pruned_b)),
        PredicateNode::make(greater_than_(mul_(pruned_a, 2), 5),
          ProjectionNode::make(expression_vector(pruned_a, pruned_b, mul_(pruned_a, 2)),
           pruned_node_a)))));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, ProjectionDoesNotRecompute) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(add_(a, 2), 1)),
    PredicateNode::make(greater_than_(add_(a, 2), 5),
      ProjectionNode::make(expression_vector(add_(a, 2)),
        node_a)));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(add_(pruned_a, 2), 1)),
    PredicateNode::make(greater_than_(add_(pruned_a, 2), 5),
      ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
        pruned_node_a)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, Diamond) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  const auto sub_lqp = ProjectionNode::make(expression_vector(add_(a, 2), add_(b, 3), add_(c, 4)),
        node_a);

  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2), add_(b, 3)),
    UnionNode::make(UnionMode::All,
      PredicateNode::make(greater_than_(add_(a, 2), 5),
        sub_lqp),
      PredicateNode::make(less_than_(add_(b, 3), 10),
        sub_lqp)));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto expected_sub_lqp = ProjectionNode::make(expression_vector(add_(pruned_a, 2), add_(pruned_b, 3)),
        pruned_node_a);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2), add_(pruned_b, 3)),
    UnionNode::make(UnionMode::All,
      PredicateNode::make(greater_than_(add_(pruned_a, 2), 5),
        expected_sub_lqp),
      PredicateNode::make(less_than_(add_(pruned_b, 3), 10),
        expected_sub_lqp)));
  // clang-format on

  // We can be sure that the top projection node does not recompute a+2 because a is not available

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, SimpleAggregate) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(add_(a, 2))),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(), expression_vector(sum_(add_(pruned_a, 2))),
    ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, UngroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(node_a)),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(), expression_vector(count_star_(pruned_node_a)),
    ProjectionNode::make(expression_vector(pruned_a, pruned_b, add_(pruned_a, 2)),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, GroupedCountStar) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  // clang-format off
  lqp =
  AggregateNode::make(expression_vector(b, a), expression_vector(count_star_(node_a)),
    ProjectionNode::make(expression_vector(a, b, add_(a, 2)),
      node_a));

  // Create deep copy so we can set pruned ColumnIDs on node_a below without manipulating the input LQP
  lqp = lqp->deep_copy();

  const auto pruned_node_a = pruned(node_a, {ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");
  const auto pruned_b = pruned_node_a->get_column("b");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(pruned_b, pruned_a), expression_vector(count_star_(pruned_node_a)),
    ProjectionNode::make(expression_vector(pruned_a, pruned_b, add_(pruned_a, 2)),
      pruned_node_a));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, InnerJoinToSemiJoin) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Semi, equals_(pruned_a, column0),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, InnerJoinToSemiJoinTwoPredicates) {
  // Same as InnerJoinToSemiJoin, but with an additional join predicate that should not change the result
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");
  const auto column1 = stored_table_node->get_column("column1");

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, expression_vector(equals_(a, column0), equals_(a, column1)),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Semi, expression_vector(equals_(pruned_a, column0), equals_(pruned_a, column1)),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotTouchInnerJoinWithNonEqui) {
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::No);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, greater_than_(a, column0),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  // Still expect it to prune b+1
  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Inner, greater_than_(pruned_a, column0),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotTouchInnerJoinWithoutUniqueConstraint) {
  // based on the InnerJoinToSemiJoin test
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(pruned_a, column0),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotTouchNonInnerJoin) {
  // based on the InnerJoinToSemiJoin test
  auto lqp = std::shared_ptr<AbstractLQPNode>{};

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_unique_constraint({ColumnID{0}}, IsPrimaryKey::Yes);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Left, equals_(a, column0),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Left, equals_(pruned_a, column0),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneUpdateInputs) {
  // Do not prune away input columns to Update, Update needs them all

  // clang-format off
  const auto select_rows_lqp =
  PredicateNode::make(greater_than_(a, 5),
    node_a);

  const auto lqp =
  UpdateNode::make("dummy",
    select_rows_lqp,
    ProjectionNode::make(expression_vector(a, add_(b, 1), c),
      select_rows_lqp));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneInsertInputs) {
  // Do not prune away input columns to Insert, Insert needs them all

  // clang-format off
  const auto lqp =
  InsertNode::make("dummy",
    PredicateNode::make(greater_than_(a, 5),
      node_a));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ColumnPruningRuleTest, DoNotPruneDeleteInputs) {
  // Do not prune away input columns to Delete, Delete needs them all

  // clang-format off
  const auto lqp =
  DeleteNode::make(
    PredicateNode::make(greater_than_(a, 5),
      node_a));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
