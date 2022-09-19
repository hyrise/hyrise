#include "strategy_base_test.hpp"

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

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

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

  const std::shared_ptr<MockNode> pruned(const std::shared_ptr<MockNode> node,
                                         const std::vector<ColumnID>& column_ids) {
    const auto pruned_node = std::static_pointer_cast<MockNode>(node->deep_copy());
    pruned_node->set_pruned_column_ids(column_ids);
    return pruned_node;
  }

  std::shared_ptr<JoinToSemiJoinRule> rule;
  std::shared_ptr<MockNode> node_a, node_b;
  std::shared_ptr<LQPColumnExpression> a, b, c, u, v, w;
};

TEST_F(JoinToSemiJoinRuleTest, InnerJoinToSemiJoin) {
  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, equals_(a, column0),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Semi, equals_(pruned_a, column0),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, MultiPredicateInnerJoinToSemiJoinWithSingleEqui) {
  // Same as InnerJoinToSemiJoin, but with an additional join predicate that should not change the result.

  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");
  const auto column1 = stored_table_node->get_column("column1");

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, expression_vector(equals_(a, column0), not_equals_(a, column1)),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
      stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Semi, expression_vector(equals_(pruned_a, column0), not_equals_(pruned_a, column1)),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(JoinToSemiJoinRuleTest, MultiPredicateInnerJoinToSemiJoinWithMultiEqui) {
  /**
   * Defines a multi-column key constraint (column0, column1) and two inner join predicates of type Equals covering
   * those two columns. We expect to see a semi join reformulation because the resulting unique constraint matches
   * the inner join's predicate expressions.
   */
  {
    TableColumnDefinitions column_definitions;
    column_definitions.emplace_back("column0", DataType::Int, false);
    column_definitions.emplace_back("column1", DataType::Int, false);
    auto table = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);

    auto& sm = Hyrise::get().storage_manager;
    sm.add_table("table", table);

    table->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
  }

  const auto stored_table_node = StoredTableNode::make("table");
  const auto column0 = stored_table_node->get_column("column0");
  const auto column1 = stored_table_node->get_column("column1");

  // clang-format off
  const auto lqp =
  ProjectionNode::make(expression_vector(add_(a, 2)),
    JoinNode::make(JoinMode::Inner, expression_vector(equals_(a, column0), equals_(a, column1)),
      ProjectionNode::make(expression_vector(a, add_(b, 1)),
        node_a),
    stored_table_node));

  const auto pruned_node_a = pruned(node_a, {ColumnID{1}, ColumnID{2}});
  const auto pruned_a = pruned_node_a->get_column("a");

  const auto annotated_lqp = apply_rule(std::make_shared<ColumnPruningRule>(), lqp);
  const auto actual_lqp = apply_rule(rule, annotated_lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(pruned_a, 2)),
    JoinNode::make(JoinMode::Semi, expression_vector(equals_(pruned_a, column0), equals_(pruned_a, column1)),
      ProjectionNode::make(expression_vector(pruned_a),
        pruned_node_a),
      stored_table_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace hyrise
