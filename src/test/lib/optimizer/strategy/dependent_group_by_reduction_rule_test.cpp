#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/dependent_group_by_reduction_rule.hpp"
#include "strategy_base_test.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class DependentGroupByReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    TableColumnDefinitions column_definitions{{"column0", DataType::Int, false},
                                              {"column1", DataType::Int, false},
                                              {"column2", DataType::Int, false},
                                              {"column3", DataType::Int, false}};

    table_a = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    table_a->add_soft_constraint(TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY});
    storage_manager.add_table("table_a", table_a);
    stored_table_node_a = StoredTableNode::make("table_a");
    column_a_0 = stored_table_node_a->get_column("column0");
    column_a_1 = stored_table_node_a->get_column("column1");
    column_a_2 = stored_table_node_a->get_column("column2");

    table_b = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    table_b->add_soft_constraint(TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
    storage_manager.add_table("table_b", table_b);
    stored_table_node_b = StoredTableNode::make("table_b");
    column_b_0 = stored_table_node_b->get_column("column0");
    column_b_1 = stored_table_node_b->get_column("column1");
    column_b_2 = stored_table_node_b->get_column("column2");

    table_c = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    table_c->add_soft_constraint(TableKeyConstraint{{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});
    storage_manager.add_table("table_c", table_c);
    stored_table_node_c = StoredTableNode::make("table_c");
    column_c_0 = stored_table_node_c->get_column("column0");
    column_c_1 = stored_table_node_c->get_column("column1");
    column_c_2 = stored_table_node_c->get_column("column2");

    table_d = std::make_shared<Table>(TableColumnDefinitions{{"column0", DataType::Int, false}}, TableType::Data,
                                      ChunkOffset{2}, UseMvcc::Yes);
    storage_manager.add_table("table_d", table_d);
    stored_table_node_d = StoredTableNode::make("table_d");
    column_d_0 = stored_table_node_d->get_column("column0");

    table_e = std::make_shared<Table>(column_definitions, TableType::Data, ChunkOffset{2}, UseMvcc::Yes);
    table_e->add_soft_constraint(TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY});
    table_e->add_soft_constraint(TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE});
    storage_manager.add_table("table_e", table_e);
    stored_table_node_e = StoredTableNode::make("table_e");
    column_e_0 = stored_table_node_e->get_column("column0");
    column_e_1 = stored_table_node_e->get_column("column1");
    column_e_2 = stored_table_node_e->get_column("column2");
    column_e_3 = stored_table_node_e->get_column("column3");

    rule = std::make_shared<DependentGroupByReductionRule>();
  }

  std::shared_ptr<DependentGroupByReductionRule> rule;

  std::shared_ptr<Table> table_a, table_b, table_c, table_d, table_e;
  std::shared_ptr<StoredTableNode> stored_table_node_a, stored_table_node_b, stored_table_node_c, stored_table_node_d,
      stored_table_node_e;
  std::shared_ptr<LQPColumnExpression> column_a_0, column_a_1, column_a_2;
  std::shared_ptr<LQPColumnExpression> column_b_0, column_b_1, column_b_2;
  std::shared_ptr<LQPColumnExpression> column_c_0, column_c_1, column_c_2;
  std::shared_ptr<LQPColumnExpression> column_d_0;
  std::shared_ptr<LQPColumnExpression> column_e_0, column_e_1, column_e_2, column_e_3;
};

// Test simple cases.
TEST_F(DependentGroupByReductionRuleTest, SimpleCases) {
  // Early out for LQP without any aggregates
  {
    _lqp = PredicateNode::make(equals_(column_a_0, 17), stored_table_node_a);

    const auto expected_lqp = _lqp->deep_copy();
    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }

  // Early out for LQP where table does not have a unique column combination.
  {
    _lqp = AggregateNode::make(expression_vector(column_d_0), expression_vector(sum_(column_d_0)), stored_table_node_d);

    const auto expected_lqp = _lqp->deep_copy();
    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
}

// Test that a removable column is removed when a unary UCC is present. Check for the restored column order.
TEST_F(DependentGroupByReductionRuleTest, SingleKeyReduction) {
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_2)),
      stored_table_node_a);

    const auto expected_lqp =
    ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_2)),
      AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_2), any_(column_a_1)),
        stored_table_node_a));
    // clang-format on

    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_a_2)),  // NOLINT(whitespace/line_length)
      stored_table_node_a);

    const auto expected_lqp =
    ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_0), sum_(column_a_1), sum_(column_a_2)),  // NOLINT(whitespace/line_length)
      AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_a_2), any_(column_a_1)),  // NOLINT(whitespace/line_length)
        stored_table_node_a));
    // clang-format on

    _apply_rule(rule, _lqp);

    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
}

// Test that a non-UCC column is not removed if the full UCC is not present in the group by list.
TEST_F(DependentGroupByReductionRuleTest, IncompleteKey) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_2), expression_vector(sum_(column_b_0), sum_(column_b_1), sum_(column_b_2)),  // NOLINT(whitespace/line_length)
    stored_table_node_b);
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Test that a group by with the full (multi-column) UCC is not altered.
TEST_F(DependentGroupByReductionRuleTest, FullKeyGroupBy) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_1), expression_vector(sum_(column_b_0), sum_(column_b_1), sum_(column_b_2)),  // NOLINT(whitespace/line_length)
    stored_table_node_b);
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Test adaption of multi-column but inconsecutive column order of the primary key columns (table_c with UCC {0,2}).
TEST_F(DependentGroupByReductionRuleTest, FullInconsecutiveKeyGroupBy) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_c_0, column_c_1, column_c_2), expression_vector(sum_(column_c_1)),
    stored_table_node_c);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_c_0, column_c_1, column_c_2, sum_(column_c_1)),
    AggregateNode::make(expression_vector(column_c_0, column_c_2), expression_vector(sum_(column_c_1), any_(column_c_1)),  // NOLINT(whitespace/line_length)
      stored_table_node_c));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Test whether we remove the correct columns after joining (one column of a and b's join key can be removed). No
// projection added, as root already is a projection.
TEST_F(DependentGroupByReductionRuleTest, JoinSingleKeyPrimaryKey) {
  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_0, column_a_1, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2)),  // NOLINT(whitespace/line_length)
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_b_0),
        stored_table_node_a,
        stored_table_node_b)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2), any_(column_a_1), any_(column_b_0)),  // NOLINT(whitespace/line_length)
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_b_0),
        stored_table_node_a,
        stored_table_node_b)));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Similar to JoinSingleKeyPrimaryKey, but we cannot get a new FD from the join keys. Thus, we cannot remove b's join
// key.
TEST_F(DependentGroupByReductionRuleTest, JoinSingleKeyPrimaryKeyNoEquiPredicate) {
  // clang-format off
  _lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_0, column_a_1, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2)),  // NOLINT(whitespace/line_length)
      JoinNode::make(JoinMode::Inner, greater_than_(column_a_0, column_b_0),
        stored_table_node_a,
        stored_table_node_b)));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_0, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2), any_(column_a_1)),  // NOLINT(whitespace/line_length)
      JoinNode::make(JoinMode::Inner, greater_than_(column_a_0, column_b_0),
        stored_table_node_a,
        stored_table_node_b)));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Test that the plan stays the same (no alias, no projection) for a table with a primary key but no removable columns.
TEST_F(DependentGroupByReductionRuleTest, AggregateButNoChanges) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0)),
    stored_table_node_a);
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// A simple aggregate follows an optimized aggregate, column order of root node should not change. Thus, no projection.
TEST_F(DependentGroupByReductionRuleTest, SimpleAggregateFollowsAdaptedAggregate) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_1)),
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_0)),
      stored_table_node_a));

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_1)),
    AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0), any_(column_a_1)),
      stored_table_node_a));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// A sort follows an optimized aggregate. Operator following the aggregate does not change the column order itself, but
// aggregate does. Hence, we need to add a projection.
TEST_F(DependentGroupByReductionRuleTest, SortFollowsAggregate) {
  // clang-format off
  _lqp =
  SortNode::make(expression_vector(column_a_0), std::vector<SortMode>{SortMode::Ascending},
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_0)),
      stored_table_node_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_0)),
    SortNode::make(expression_vector(column_a_0), std::vector<SortMode>{SortMode::Ascending},
      AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0), any_(column_a_1)),
        stored_table_node_a)));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// When a primary key column is nullable after an outer join, check that we do not modify the aggregate.
TEST_F(DependentGroupByReductionRuleTest, NoAdaptionForNullableColumns) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_a_0, column_a_1, column_b_2), expression_vector(sum_(column_a_0)),
    JoinNode::make(JoinMode::FullOuter, equals_(column_a_0, column_b_0),
      stored_table_node_a,
      stored_table_node_b));
  // clang-format on

  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Check that we reduce using the shortest UCCs (in terms of the number of columns).
TEST_F(DependentGroupByReductionRuleTest, ShortConstraintsFirst) {
  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(column_e_0, column_e_1, column_e_2), expression_vector(min_(column_e_3)),
    stored_table_node_e);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_e_0, column_e_1, column_e_2, min_(column_e_3)),
    AggregateNode::make(expression_vector(column_e_2), expression_vector(min_(column_e_3), any_(column_e_1), any_(column_e_0)),  // NOLINT(whitespace/line_length)
      stored_table_node_e));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

// Check whether we can reduce the group-by list twice.
TEST_F(DependentGroupByReductionRuleTest, MultiKeyReduction) {
  // Since this is a special FD-scenario that can not be generated from UniqueConstraints and StoredTableNodes at the
  // moment, we have to use a custom MockNode:
  const auto mock_node = MockNode::make(MockNode::ColumnDefinitions{
      {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}, {DataType::Int, "e"}});
  const auto a = mock_node->get_column("a");
  const auto b = mock_node->get_column("b");
  const auto c = mock_node->get_column("c");
  const auto d = mock_node->get_column("d");
  const auto e = mock_node->get_column("e");
  const auto fd_a = FunctionalDependency{{a}, {b}};
  const auto fd_c = FunctionalDependency{{c}, {d}};
  mock_node->set_non_trivial_functional_dependencies({fd_a, fd_c});

  // clang-format off
  _lqp =
  AggregateNode::make(expression_vector(a, b, c, d), expression_vector(sum_(e)),
    mock_node);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a, b, c, d, sum_(e)),
    AggregateNode::make(expression_vector(a, c), expression_vector(sum_(e), any_(b), any_(d)),
      mock_node));
  // clang-format on

  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(DependentGroupByReductionRuleTest, RemoveSuperfluousDistinctAggregateSimple) {
  // To guarantee distinct results for SELECT DISTINCT clauses, the SQLTranslator adds an AggregateNode with the
  // selected attributes as group by columns. If the AggregateNode's input is already unique for these columns,
  // remove the entire node.

  // Basic case: Column is unique due to a constraint (e.g., it is a primary key).
  // Example query: SELECT DISTINCT column0 FROM table_a;
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_0), expression_vector(),
      stored_table_node_a);
    // clang-format on

    stored_table_node_a->set_pruned_column_ids({ColumnID{1}, ColumnID{2}, ColumnID{3}});

    const auto expected_lqp = stored_table_node_a->deep_copy();
    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }

  // More advanced case: Column is unique due to another operation (e.g., we grouped by it before).
  // Example query: SELECT DISTINCT column1, MIN(column2) FROM table_a GROUP BY column1;
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_1, min_(column_a_2)), expression_vector(),
      AggregateNode::make(expression_vector(column_a_1), expression_vector(min_(column_a_2)),
        stored_table_node_a));

    const auto expected_lqp =
    AggregateNode::make(expression_vector(column_a_1), expression_vector(min_(column_a_2)),
      stored_table_node_a);
    // clang-format on

    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
}

TEST_F(DependentGroupByReductionRuleTest, RemoveSuperfluousDistinctAggregateProjectColumns) {
  // Remove the AggregateNode completely when it is used to for SELECT DISTINCT on a unique column, but add a
  // ProjectionNode to output only the desired column.
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_0), expression_vector(),
      stored_table_node_a);

    const auto expected_lqp =
    ProjectionNode::make(expression_vector(column_a_0),
      stored_table_node_a);
    // clang-format on

    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }

  // All columns are part of the grouped columns, but the order changes. Thus, we need a ProjectionNode that changes the
  // order.
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_e_3, column_e_2, column_e_1, column_e_0), expression_vector(),
      stored_table_node_e);

    const auto expected_lqp =
    ProjectionNode::make(expression_vector(column_e_3, column_e_2, column_e_1, column_e_0),
      stored_table_node_e);
    // clang-format on

    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
}

TEST_F(DependentGroupByReductionRuleTest, DoNotRemoveRequiredDistinctAggregate) {
  // Do not remove the AggregateNode when the grouped column is not unique (only a combination of the two columns
  // column0 and column1 is unique).
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_b_0), expression_vector(),
      stored_table_node_b);
    // clang-format on

    const auto expected_lqp = _lqp->deep_copy();
    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }

  // Do not remove the AggregateNode when the grouped column is unique but there are further aggregates.
  {
    // clang-format off
    _lqp =
    AggregateNode::make(expression_vector(column_a_0), expression_vector(min_(column_a_1)),
      stored_table_node_a);
    // clang-format on

    const auto expected_lqp = _lqp->deep_copy();
    _apply_rule(rule, _lqp);
    EXPECT_LQP_EQ(_lqp, expected_lqp);
  }
}

}  // namespace hyrise
