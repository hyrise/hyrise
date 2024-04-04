#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/change_meta_table_node.hpp"
#include "logical_query_plan/delete_node.hpp"
#include "logical_query_plan/export_node.hpp"
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
#include "optimizer/strategy/join_to_semi_join_rule.hpp"
#include "strategy_base_test.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class JoinToSemiJoinRuleTest : public StrategyBaseTest {
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

    rule = std::make_shared<JoinToSemiJoinRule>();
  }

  std::shared_ptr<JoinToSemiJoinRule> rule;
  std::shared_ptr<MockNode> node_a;
  std::shared_ptr<MockNode> node_b;
  std::shared_ptr<LQPColumnExpression> a, b, c, u, v, w;
};

TEST_F(JoinToSemiJoinRuleTest, InnerJoinToSemiJoin) {
  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Semi, equals_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, MultiPredicateInnerJoinToSemiJoinWithSingleEqui) {
  // Same as InnerJoinToSemiJoin, but with an additional join predicate that should not change the result.

  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");
  const auto column1 = stored_table_node->get_column("column1");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, expression_vector(equals_(a, column0), not_equals_(a, column1)),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Semi, expression_vector(equals_(a, column0), not_equals_(a, column1)),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, MultiPredicateInnerJoinToSemiJoinWithMultiEqui) {
  /**
   * Defines a multi-column UCC (column0, column1) and two inner join predicates of type Equals covering these two
   * columns. We expect to see a semi join reformulation because the resulting unique column combination matches the
   * inner join's predicate expressions.
   */
  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");
  const auto column1 = stored_table_node->get_column("column1");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, expression_vector(equals_(a, column0), equals_(a, column1)),
      ProjectionNode::make(expression_vector(a),
        node_a),
    stored_table_node));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Semi, expression_vector(equals_(a, column0), equals_(a, column1)),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, DoNotTouchInnerJoinWithNonEqui) {
  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, greater_than_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, DoNotTouchInnerJoinWithoutUcc) {
  // Based on the InnerJoinToSemiJoin test.
  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, DoNotTouchInnerJoinWithoutMatchingUcc) {
  /**
   * Based on the InnerJoinToSemiJoin test.
   *
   * We define a multi-column UCC (column0, column1), but only a single Equals-predicate for the inner join
   * (a == column0). Hence, the resulting unique column combination does not match the expressions of the single equals
   * predicate and we should not see a semi join reformulation.
   */

  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, DoNotTouchNonInnerJoin) {
  // Based on the InnerJoinToSemiJoin test.
  {
    auto column_definitions = TableColumnDefinitions{};
    column_definitions.emplace_back("column0", DataType::Int, false);
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Left, equals_(a, column0),
      ProjectionNode::make(expression_vector(a),
        node_a),
      stored_table_node));
  // clang-format on

  static_cast<JoinNode&>(*_lqp->left_input()).mark_input_side_as_prunable(LQPInputSide::Right);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

}  // namespace hyrise
