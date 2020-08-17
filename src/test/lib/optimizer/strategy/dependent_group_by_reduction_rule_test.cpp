#include "strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/dependent_group_by_reduction_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class DependentGroupByReductionRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    auto& storage_manager = Hyrise::get().storage_manager;

    TableColumnDefinitions column_definitions{
        {"column0", DataType::Int, false}, {"column1", DataType::Int, false}, {"column2", DataType::Int, false}};

    table_a = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_a->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY});
    storage_manager.add_table("table_a", table_a);
    stored_table_node_a = StoredTableNode::make("table_a");
    column_a_0 = stored_table_node_a->get_column("column0");
    column_a_1 = stored_table_node_a->get_column("column1");
    column_a_2 = stored_table_node_a->get_column("column2");

    table_b = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_b->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});
    storage_manager.add_table("table_b", table_b);
    stored_table_node_b = StoredTableNode::make("table_b");
    column_b_0 = stored_table_node_b->get_column("column0");
    column_b_1 = stored_table_node_b->get_column("column1");
    column_b_2 = stored_table_node_b->get_column("column2");

    table_c = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_c->add_soft_key_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::PRIMARY_KEY});
    storage_manager.add_table("table_c", table_c);
    stored_table_node_c = StoredTableNode::make("table_c");
    column_c_0 = stored_table_node_c->get_column("column0");
    column_c_1 = stored_table_node_c->get_column("column1");
    column_c_2 = stored_table_node_c->get_column("column2");

    table_d = std::make_shared<Table>(TableColumnDefinitions{{"column0", DataType::Int, false}}, TableType::Data, 2,
                                      UseMvcc::Yes);
    storage_manager.add_table("table_d", table_d);
    stored_table_node_d = StoredTableNode::make("table_d");
    column_d_0 = stored_table_node_d->get_column("column0");

    table_e = std::make_shared<Table>(column_definitions, TableType::Data, 2, UseMvcc::Yes);
    table_e->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY});
    table_e->add_soft_key_constraint({{ColumnID{2}}, KeyConstraintType::UNIQUE});
    storage_manager.add_table("table_e", table_e);
    stored_table_node_e = StoredTableNode::make("table_e");
    column_e_0 = stored_table_node_e->get_column("column0");
    column_e_1 = stored_table_node_e->get_column("column1");
    column_e_2 = stored_table_node_e->get_column("column2");

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
  std::shared_ptr<LQPColumnExpression> column_e_0, column_e_1, column_e_2;
};

// Test simple cases
TEST_F(DependentGroupByReductionRuleTest, SimpleCases) {
  // Early out for LQP without any aggregates
  {
    const auto lqp = PredicateNode::make(equals_(column_a_0, 17), stored_table_node_a);

    const auto actual_lqp = apply_rule(rule, lqp);
    const auto expected_lqp = lqp->deep_copy();
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }

  // Early out for LQP where table does not have a key constraint
  {
    const auto lqp =
        AggregateNode::make(expression_vector(column_d_0), expression_vector(sum_(column_d_0)), stored_table_node_d);

    const auto actual_lqp = apply_rule(rule, lqp);
    const auto expected_lqp = lqp->deep_copy();
    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
}

// Test that a removable column is removed when a single column primary key is present.
// Check for the restored column order.
TEST_F(DependentGroupByReductionRuleTest, SingleKeyReduction) {
  // clang-format off
  {
    auto lqp =
      AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_2)),
        stored_table_node_a);  // NOLINT

    const auto actual_lqp = apply_rule(rule, lqp);

    const auto expected_lqp =
      ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_2)),
      AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_2), any_(column_a_1)),
        stored_table_node_a));  // NOLINT

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  {
    auto lqp =
    AggregateNode::make(expression_vector(column_a_0, column_a_1),
                        expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_a_2)),
      stored_table_node_a);  // NOLINT

    const auto actual_lqp = apply_rule(rule, lqp);

    const auto expected_lqp =
    ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_0), sum_(column_a_1),
                                                                                                  sum_(column_a_2)),
      AggregateNode::make(expression_vector(column_a_0),
                          expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_a_2), any_(column_a_1)),
        stored_table_node_a));  // NOLINT

    EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  }
  // clang-format on
}

// Test that a non-primary-key column is not removed if the full key is not present in the group by list.
TEST_F(DependentGroupByReductionRuleTest, IncompleteKey) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_2), expression_vector(sum_(column_b_0), sum_(column_b_1), sum_(column_b_2)), stored_table_node_b);  // NOLINT
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that a group by with the full (multi-column) key constraint is not altered.
TEST_F(DependentGroupByReductionRuleTest, FullKeyGroupBy) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_b_0, column_b_1), expression_vector(sum_(column_b_0), sum_(column_b_1), sum_(column_b_2)), stored_table_node_b);  // NOLINT
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test adaption of multi-column and but inconsecutive column order primary key columns (table_c with {0,2})
TEST_F(DependentGroupByReductionRuleTest, FullInconsecutiveKeyGroupBy) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_c_0, column_c_1, column_c_2), expression_vector(sum_(column_c_1)), stored_table_node_c);  // NOLINT

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_c_0, column_c_1, column_c_2, sum_(column_c_1)),
    AggregateNode::make(expression_vector(column_c_0, column_c_2), expression_vector(sum_(column_c_1), any_(column_c_1)), stored_table_node_c));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test whether we remove the correct columns after joining (one column of a can be moved, none of b).
// No projection added as root already is a projection.
TEST_F(DependentGroupByReductionRuleTest, JoinSingleKeyPrimaryKey) {
  // clang-format off
  auto lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_0, column_a_1, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2)),  // NOLINT
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_b_0), stored_table_node_a, stored_table_node_b)));

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(add_(column_a_0, 5), add_(column_a_1, 5), sum_(column_b_2)),
    AggregateNode::make(expression_vector(column_a_0, column_b_0, column_b_2), expression_vector(sum_(column_a_0), sum_(column_a_1), sum_(column_b_2), any_(column_a_1)),  // NOLINT
      JoinNode::make(JoinMode::Inner, equals_(column_a_0, column_b_0), stored_table_node_a, stored_table_node_b)));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Test that the plan stays the same (no alias, no projection) for a table with a primary key but no removable columns
TEST_F(DependentGroupByReductionRuleTest, AggregateButNoChanges) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0)), stored_table_node_a);
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// A simple aggregate follows an optimized aggregate, column order of root node should not change. Thus no projection.
TEST_F(DependentGroupByReductionRuleTest, SimpleAggregateFollowsAdaptedAggregate) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_1)),
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_0)), stored_table_node_a));  // NOLINT

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_1)),
    AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0), any_(column_a_1)), stored_table_node_a));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// A sort follows an optimized aggregate. Operator following the aggregate does not change the column order itself, but
// aggregate does. Hence, we need to add a projection.
TEST_F(DependentGroupByReductionRuleTest, SortFollowsAggregate) {
  // clang-format off
  auto lqp =
  SortNode::make(expression_vector(column_a_0), std::vector<SortMode>{SortMode::Ascending},
    AggregateNode::make(expression_vector(column_a_0, column_a_1), expression_vector(sum_(column_a_0)), stored_table_node_a));  // NOLINT

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_a_0, column_a_1, sum_(column_a_0)),
    SortNode::make(expression_vector(column_a_0), std::vector<SortMode>{SortMode::Ascending},
      AggregateNode::make(expression_vector(column_a_0), expression_vector(sum_(column_a_0), any_(column_a_1)), stored_table_node_a)));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// When a primary key column is nullable after an outer join, check that we do not modify the aggregate.
TEST_F(DependentGroupByReductionRuleTest, NoAdaptionForNullableColumns) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_a_0, column_a_1, column_b_2), expression_vector(sum_(column_a_0)),
    JoinNode::make(JoinMode::FullOuter, equals_(column_a_0, column_b_0),
      stored_table_node_a,
      stored_table_node_b));
  // clang-format on

  const auto actual_lqp = apply_rule(rule, lqp);
  const auto expected_lqp = lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Check that we reduce using the shortest constraints (in terms of the number of columns).
TEST_F(DependentGroupByReductionRuleTest, ShortConstraintsFirst) {
  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(column_e_0, column_e_1, column_e_2), expression_vector(), stored_table_node_e);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(column_e_0, column_e_1, column_e_2),
    AggregateNode::make(expression_vector(column_e_2), expression_vector(any_(column_e_1), any_(column_e_0)), stored_table_node_e));  // NOLINT
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

// Check whether we can reduce the group-by list twice.
TEST_F(DependentGroupByReductionRuleTest, MultiKeyReduction) {
  // Since this is a special FD-scenario that can not be generated from UniqueConstraints and StoredTableNodes at the
  // moment, we have to use a custom MockNode:
  auto mock_node = MockNode::make(MockNode::ColumnDefinitions{
      {DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}, {DataType::Int, "d"}, {DataType::Int, "e"}});
  auto a = mock_node->get_column("a");
  auto b = mock_node->get_column("b");
  auto c = mock_node->get_column("c");
  auto d = mock_node->get_column("d");
  auto e = mock_node->get_column("e");
  auto fd_a = FunctionalDependency{{a}, {b}};
  auto fd_c = FunctionalDependency{{c}, {d}};
  mock_node->set_non_trivial_functional_dependencies({fd_a, fd_c});

  // clang-format off
  auto lqp =
  AggregateNode::make(expression_vector(a, b, c, d), expression_vector(sum_(e)),
    mock_node);

  const auto actual_lqp = apply_rule(rule, lqp);

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(a, b, c, d, sum_(e)),
    AggregateNode::make(expression_vector(a, c), expression_vector(sum_(e), any_(b), any_(d)),
      mock_node));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
