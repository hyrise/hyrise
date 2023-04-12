#include <memory>
#include <string>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/constraints/table_key_constraint.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_a = load_table("resources/test_data/tbl/int_int_float.tbl", ChunkOffset{1});
    ChunkEncoder::encode_all_chunks(_table_a);
    _table_b = load_table("resources/test_data/tbl/int_int_float.tbl", ChunkOffset{1});

    Hyrise::get().storage_manager.add_table("t_a", _table_a);
    Hyrise::get().storage_manager.add_table("t_b", _table_b);

    _table_a->create_chunk_index<GroupKeyIndex>({ColumnID{0}}, "i_a1");
    _table_a->create_chunk_index<GroupKeyIndex>({ColumnID{1}}, "i_b");
    _table_a->create_chunk_index<CompositeGroupKeyIndex>({ColumnID{0}, ColumnID{1}}, "i_a2");
    _table_a->create_chunk_index<CompositeGroupKeyIndex>({ColumnID{1}, ColumnID{0}}, "i_a3");

    _stored_table_node = StoredTableNode::make("t_a");
    _a = _stored_table_node->get_column("a");
    _b = _stored_table_node->get_column("b");
    _c = _stored_table_node->get_column("c");

    _stored_table_node->set_pruned_chunk_ids({ChunkID{2}});
  }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c;
  std::shared_ptr<Table> _table_a, _table_b;
};

TEST_F(StoredTableNodeTest, Description) {
  const auto stored_table_node_a = StoredTableNode::make("t_a");
  EXPECT_EQ(stored_table_node_a->description(), "[StoredTable] Name: 't_a' pruned: 0/4 chunk(s), 0/3 column(s)");

  const auto stored_table_node_b = StoredTableNode::make("t_a");
  stored_table_node_b->set_pruned_chunk_ids({ChunkID{2}});
  stored_table_node_b->set_pruned_column_ids({ColumnID{1}});
  EXPECT_EQ(stored_table_node_b->description(), "[StoredTable] Name: 't_a' pruned: 1/4 chunk(s), 1/3 column(s)");
}

TEST_F(StoredTableNodeTest, GetColumn) {
  EXPECT_EQ(*_stored_table_node->get_column("a"), *_a);
  EXPECT_EQ(*_stored_table_node->get_column("b"), *_b);

  // Column pruning does not interfere with get_column()
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});
  EXPECT_EQ(*_stored_table_node->get_column("a"), *_a);
  EXPECT_EQ(*_stored_table_node->get_column("b"), *_b);
}

TEST_F(StoredTableNodeTest, ColumnExpressions) {
  EXPECT_EQ(_stored_table_node->output_expressions().size(), 3u);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(0u), *_a);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(1u), *_b);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(2u), *_c);

  // Column pruning does not interfere with get_column()
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});
  EXPECT_EQ(_stored_table_node->output_expressions().size(), 2u);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(0u), *_b);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(1u), *_c);
}

TEST_F(StoredTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_stored_table_node, *_stored_table_node);

  const auto different_node_a = StoredTableNode::make("t_b");
  different_node_a->set_pruned_chunk_ids({ChunkID{2}});

  const auto different_node_b = StoredTableNode::make("t_a");

  const auto different_node_c = StoredTableNode::make("t_b");
  different_node_c->set_pruned_column_ids({ColumnID{1}});
  const auto different_node_c2 = StoredTableNode::make("t_b");
  different_node_c2->set_pruned_column_ids({ColumnID{1}});

  EXPECT_NE(*_stored_table_node, *different_node_a);
  EXPECT_NE(*_stored_table_node, *different_node_b);
  EXPECT_NE(*_stored_table_node, *different_node_c);
  EXPECT_EQ(*different_node_c, *different_node_c2);

  EXPECT_NE(_stored_table_node->hash(), different_node_a->hash());
  EXPECT_NE(_stored_table_node->hash(), different_node_b->hash());
  EXPECT_NE(_stored_table_node->hash(), different_node_c->hash());
  EXPECT_EQ(different_node_c->hash(), different_node_c2->hash());
}

TEST_F(StoredTableNodeTest, Copy) {
  EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node);

  _stored_table_node->set_pruned_chunk_ids({ChunkID{2}});
  _stored_table_node->set_pruned_column_ids({ColumnID{1}});
  EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node);

  const auto stored_table_node_b = StoredTableNode::make("t_b");
  const auto x = stored_table_node_b->get_column("a");

  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(min_(x)),
    AggregateNode::make(expression_vector(), expression_vector(min_(x)),
      stored_table_node_b));

  const auto lqp =
  PredicateNode::make(equals_(_a, lqp_subquery_(subquery)),
    _stored_table_node);
  // clang-format on

  _stored_table_node->set_prunable_subquery_predicates({lqp});

  const auto& lqp_deep_copy = lqp->deep_copy();
  EXPECT_EQ(*lqp, *lqp_deep_copy);
  EXPECT_EQ(*_stored_table_node, *lqp_deep_copy->left_input());

  const auto& prunable_subquery_predicates =
      static_cast<StoredTableNode&>(*lqp_deep_copy->left_input()).prunable_subquery_predicates();
  ASSERT_EQ(prunable_subquery_predicates.size(), 1);
  EXPECT_EQ(prunable_subquery_predicates.front(), lqp_deep_copy);

  // Do not allow deep copies where prunable subquery predicates are not part of the LQP.
  EXPECT_THROW(_stored_table_node->deep_copy(), std::logic_error);
}

TEST_F(StoredTableNodeTest, NodeExpressions) {
  ASSERT_EQ(_stored_table_node->node_expressions.size(), 0u);
}

TEST_F(StoredTableNodeTest, GetStatisticsPruneFirstColumn) {
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 4u);

  auto expected_statistics = _stored_table_node->chunk_indexes_statistics().at(1u);

  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  // column with ColumnID{0} was pruned, therefore the column has to be left shifted
  expected_statistics.column_ids[0] -= 1;

  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 1u);
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().at(0u), expected_statistics);
}

TEST_F(StoredTableNodeTest, GetStatisticsPruneSecondColumn) {
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 4u);

  auto expected_statistics = _stored_table_node->chunk_indexes_statistics().at(0u);

  _stored_table_node->set_pruned_column_ids({ColumnID{1}});

  // column with ColumnID{1} was pruned, so ColumnID{0} should be untouched

  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 1u);
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().at(0u), expected_statistics);
}

TEST_F(StoredTableNodeTest, GetStatisticsPruneBothColumns) {
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 4u);

  _stored_table_node->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});

  // All indexed columns were pruned, therefore the index statistics should be empty
  EXPECT_EQ(_stored_table_node->chunk_indexes_statistics().size(), 0u);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesNone) {
  // No constraints => No functional dependencies
  EXPECT_TRUE(_stored_table_node->functional_dependencies().empty());

  // Constraint across all columns => No more columns available to create a functional dependency from
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint(
      {{_a->original_column_id, _b->original_column_id, _c->original_column_id}, KeyConstraintType::UNIQUE});

  EXPECT_TRUE(_stored_table_node->functional_dependencies().empty());
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesSingle) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});

  const auto& fds = _stored_table_node->functional_dependencies();
  const auto fd_expected = FunctionalDependency{{_a}, {_b, _c}};

  EXPECT_EQ(fds.size(), 1);
  EXPECT_TRUE(fds.contains(fd_expected));
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedLeftColumnSet) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});

  // Prune unique column "a", which would be part of the left column set in the resulting FD: {a} => {b, c}
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  EXPECT_TRUE(_stored_table_node->functional_dependencies().empty());
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedLeftColumnSet2) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint({{_b->original_column_id}, KeyConstraintType::UNIQUE});

  // Prune unique column "a", which would be part of the left column set in the resulting FD: {a} => {b, c}
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  const auto fd_expected = FunctionalDependency{{_b}, {_c}};
  EXPECT_EQ(_stored_table_node->functional_dependencies().size(), 1);
  EXPECT_TRUE(_stored_table_node->functional_dependencies().contains(fd_expected));
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedRightColumnSet) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});

  // Prune column "b", which would be part of the right column set in the resulting FD: {a} => {b, c}
  _stored_table_node->set_pruned_column_ids({ColumnID{1}});

  const auto fd_expected = FunctionalDependency{{_a}, {_c}};
  EXPECT_EQ(_stored_table_node->functional_dependencies().size(), 1);
  EXPECT_TRUE(_stored_table_node->functional_dependencies().contains(fd_expected));
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesMultiple) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");  // int_int_float.tbl
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});
  table->add_soft_key_constraint({{_a->original_column_id, _b->original_column_id}, KeyConstraintType::UNIQUE});

  const auto& fds = _stored_table_node->functional_dependencies();

  const auto fd1_expected = FunctionalDependency{{_a}, {_b, _c}};
  const auto fd2_expected = FunctionalDependency{{_a, _b}, {_c}};

  EXPECT_EQ(fds.size(), 2);
  EXPECT_TRUE(fds.contains(fd1_expected));
  EXPECT_TRUE(fds.contains(fd2_expected));
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesExcludeNullableColumns) {
  // Create four identical tables of 3 columns (a, b, c), where the second column of which is nullable (b)
  TableColumnDefinitions column_definitions{
      {"a", DataType::Int, false}, {"b", DataType::Int, true}, {"c", DataType::Int, false}};

  // Test {a} => {b, c}
  {
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::UNIQUE});

    Hyrise::get().storage_manager.add_table("table_a", table);
    const auto stored_table_node = StoredTableNode::make("table_a");
    const auto& a = stored_table_node->get_column("a");
    const auto& b = stored_table_node->get_column("b");
    const auto& c = stored_table_node->get_column("c");
    const auto& fds = stored_table_node->functional_dependencies();

    const auto fd_expected = FunctionalDependency{{a}, {b, c}};
    EXPECT_EQ(fds.size(), 1);
    EXPECT_TRUE(fds.contains(fd_expected));
  }

  // Test {a, b} => {c}
  {
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->add_soft_key_constraint({{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE});

    Hyrise::get().storage_manager.add_table("table_b", table);
    const auto& stored_table_node = StoredTableNode::make("table_b");

    EXPECT_EQ(stored_table_node->functional_dependencies().size(), 0);
  }

  // Test {a, c} => {b}
  {
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->add_soft_key_constraint({{ColumnID{0}, ColumnID{2}}, KeyConstraintType::UNIQUE});

    Hyrise::get().storage_manager.add_table("table_c", table);
    const auto& stored_table_node = StoredTableNode::make("table_c");
    const auto& a = stored_table_node->get_column("a");
    const auto& b = stored_table_node->get_column("b");
    const auto& c = stored_table_node->get_column("c");
    const auto& fds = stored_table_node->functional_dependencies();

    const auto fd_expected = FunctionalDependency{{a, c}, {b}};
    EXPECT_EQ(fds.size(), 1);
    EXPECT_TRUE(fds.contains(fd_expected));
  }

  // Test {b} => {a, c}
  {
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->add_soft_key_constraint({{ColumnID{1}}, KeyConstraintType::UNIQUE});

    Hyrise::get().storage_manager.add_table("table_d", table);
    const auto& stored_table_node = StoredTableNode::make("table_d");

    EXPECT_TRUE(stored_table_node->functional_dependencies().empty());
  }
}

TEST_F(StoredTableNodeTest, UniqueColumnCombinations) {
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _table_a->add_soft_key_constraint(key_constraint_a_b);
  _table_a->add_soft_key_constraint(key_constraint_c);

  const auto& unique_column_combinations = _stored_table_node->unique_column_combinations();

  // Basic check.
  EXPECT_EQ(unique_column_combinations.size(), 2);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a_b, unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));

  // Check whether StoredTableNode is referenced by the UCC's expressions.
  for (const auto& ucc : unique_column_combinations) {
    for (const auto& expression : ucc.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node.expired());
      EXPECT_TRUE(column_expression->original_node.lock() == _stored_table_node);
    }
  }
}

TEST_F(StoredTableNodeTest, UniqueColumnCombinationsPrunedColumns) {
  // Prepare UCCs.
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _table_a->add_soft_key_constraint(key_constraint_a);
  _table_a->add_soft_key_constraint(key_constraint_a_b);
  _table_a->add_soft_key_constraint(key_constraint_c);
  const auto& table_key_constraints = _table_a->soft_key_constraints();
  EXPECT_EQ(table_key_constraints.size(), 3);
  EXPECT_EQ(_stored_table_node->unique_column_combinations().size(), 3);

  // Prune column a, which should remove two UCCs.
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  // Basic check.
  const auto& unique_column_combinations = _stored_table_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));
}

TEST_F(StoredTableNodeTest, UniqueColumnCombinationsEmpty) {
  EXPECT_TRUE(_table_a->soft_key_constraints().empty());
  EXPECT_TRUE(_stored_table_node->unique_column_combinations().empty());
}

TEST_F(StoredTableNodeTest, HasMatchingUniqueColumnCombination) {
  const auto key_constraint_a = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _table_a->add_soft_key_constraint(key_constraint_a);
  EXPECT_EQ(_stored_table_node->unique_column_combinations().size(), 1);

  // Negative test.
  // Columns are empty.
  EXPECT_THROW(_stored_table_node->has_matching_ucc({}), std::logic_error);

  // There is no matching UCC.
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_b}));
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_c}));
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_b, _c}));

  if constexpr (HYRISE_DEBUG) {
    // Columns are not part of output_expressions() (i.e., pruned).
    _stored_table_node->set_pruned_column_ids({ColumnID{0}});
    EXPECT_THROW(_stored_table_node->has_matching_ucc({_a}), std::logic_error);
  }

  // Test exact match.
  _stored_table_node->set_pruned_column_ids({});
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a}));

  // Test superset of column ids.
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a, _b}));
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a, _c}));
}

TEST_F(StoredTableNodeTest, OrderDependenciesSimple) {
  EXPECT_TRUE(_stored_table_node->order_dependencies().empty());

  // Create ODs from order constraints.
  _table_a->add_soft_order_constraint({{ColumnID{0}}, {ColumnID{1}}});
  _table_a->add_soft_order_constraint({{ColumnID{0}}, {ColumnID{2}}});
  EXPECT_EQ(_table_a->soft_order_constraints().size(), 2);

  const auto& order_dependencies = _stored_table_node->order_dependencies();
  EXPECT_EQ(order_dependencies.size(), 2);
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_a}, {_b}}));
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_a}, {_c}}));
}

TEST_F(StoredTableNodeTest, OrderDependenciesPrunedColumns) {
  // Discard ODs that involve pruned columns.
  _table_a->add_soft_order_constraint({{ColumnID{0}}, {ColumnID{1}}});
  _table_a->add_soft_order_constraint({{ColumnID{0}}, {ColumnID{2}}});
  EXPECT_EQ(_table_a->soft_order_constraints().size(), 2);

  _stored_table_node->set_pruned_column_ids({ColumnID{0}, ColumnID{2}});
  const auto& order_dependencies = _stored_table_node->order_dependencies();
  EXPECT_TRUE(order_dependencies.empty());
}

TEST_F(StoredTableNodeTest, OrderDependenciesTransitive) {
  // Construct transitive ODs, but do not run into cycles. Furthermore, do not add ODs where LHS and RHS include the
  // same column.
  _table_a->add_soft_order_constraint({{ColumnID{0}}, {ColumnID{1}}});
  _table_a->add_soft_order_constraint({{ColumnID{1}}, {ColumnID{2}}});
  _table_a->add_soft_order_constraint({{ColumnID{2}}, {ColumnID{0}}});
  EXPECT_EQ(_table_a->soft_order_constraints().size(), 3);

  const auto& order_dependencies = _stored_table_node->order_dependencies();
  EXPECT_EQ(order_dependencies.size(), 6);
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_a}, {_b}}));
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_b}, {_c}}));
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_c}, {_a}}));
  // Created from [a] |-> [b] and [b] |-> [c].
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_a}, {_c}}));
  // Created from [c] |-> [a] and [a] |-> [b].
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_c}, {_b}}));
  // Created from [b] |-> [c] and [c] |-> [a].
  EXPECT_TRUE(order_dependencies.contains(OrderDependency{{_b}, {_a}}));
}

TEST_F(StoredTableNodeTest, HasMatchingOrderDependency) {
  const auto table_c = Table::create_dummy_table({{"a", DataType::Int, false},
                                                  {"b", DataType::Int, false},
                                                  {"c", DataType::Float, false},
                                                  {"d", DataType::Float, false},
                                                  {"e", DataType::Float, false}});
  Hyrise::get().storage_manager.add_table("t_c", table_c);
  const auto stored_table_node = StoredTableNode::make("t_c");
  const auto a = stored_table_node->get_column("a");
  const auto b = stored_table_node->get_column("b");
  const auto c = stored_table_node->get_column("c");
  const auto d = stored_table_node->get_column("d");
  const auto e = stored_table_node->get_column("e");

  const auto order_constraint = TableOrderConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{2}, ColumnID{3}}};
  table_c->add_soft_order_constraint(order_constraint);
  EXPECT_EQ(stored_table_node->order_dependencies().size(), 1);

  // Negative tests.
  // Columns are empty.
  EXPECT_THROW(stored_table_node->has_matching_od({}, {c}), std::logic_error);
  EXPECT_THROW(stored_table_node->has_matching_od({a, b}, {}), std::logic_error);

  // There is no matching OD at all.
  EXPECT_FALSE(stored_table_node->has_matching_od({a}, {b}));
  EXPECT_FALSE(stored_table_node->has_matching_od({b}, {c}));
  EXPECT_FALSE(stored_table_node->has_matching_od({c}, {a}));
  EXPECT_FALSE(stored_table_node->has_matching_od({a, b}, {d}));
  EXPECT_FALSE(stored_table_node->has_matching_od({a, b}, {c, e}));
  EXPECT_FALSE(stored_table_node->has_matching_od({a, b}, {c, d, e}));
  EXPECT_FALSE(stored_table_node->has_matching_od({a}, {c, d}));

  if constexpr (HYRISE_DEBUG) {
    // Columns are not part of output_expressions() (i.e., pruned).
    stored_table_node->set_pruned_column_ids({ColumnID{0}});
    EXPECT_THROW(stored_table_node->has_matching_od({_a}, {_b}), std::logic_error);
    EXPECT_THROW(stored_table_node->has_matching_od({_b}, {_a}), std::logic_error);
  }

  // Test exact match.
  stored_table_node->set_pruned_column_ids({});
  EXPECT_TRUE(stored_table_node->has_matching_od({a, b}, {c, d}));

  // Test sub- and superset of column ids.
  EXPECT_TRUE(stored_table_node->has_matching_od({a, b, e}, {c, d}));
  EXPECT_TRUE(stored_table_node->has_matching_od({a, b}, {c}));
}

TEST_F(StoredTableNodeTest, InclusionDependenciesSimple) {
  EXPECT_TRUE(_stored_table_node->inclusion_dependencies().empty());

  // Create INDs from foreign key constraints.
  const auto dummy_table_a = Table::create_dummy_table({{"a", DataType::Int, false}});
  dummy_table_a->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{0}}, _table_a, dummy_table_a});
  const auto dummy_table_b = Table::create_dummy_table({{"b", DataType::Int, false}});
  dummy_table_b->add_soft_foreign_key_constraint({{ColumnID{1}}, {ColumnID{0}}, _table_a, dummy_table_b});

  const auto ind_a = InclusionDependency{{_a}, {ColumnID{0}}, dummy_table_a};
  const auto ind_b = InclusionDependency{{_b}, {ColumnID{0}}, dummy_table_b};
  const auto& inclusion_dependencies = _stored_table_node->inclusion_dependencies();
  EXPECT_EQ(inclusion_dependencies.size(), 2);
  EXPECT_TRUE(inclusion_dependencies.contains(ind_a));
  EXPECT_TRUE(inclusion_dependencies.contains(ind_b));
}

TEST_F(StoredTableNodeTest, InclusionDependenciesPrunedColumns) {
  // Discard INDs that involve pruned columns.
  const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
  dummy_table->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{0}}, _table_a, dummy_table});

  _stored_table_node->set_pruned_column_ids({ColumnID{0}});
  EXPECT_TRUE(_stored_table_node->inclusion_dependencies().empty());
}

TEST_F(StoredTableNodeTest, InclusionDependenciesTableDeleted) {
  // Discard INDs that reference a table that was deleted.
  {
    const auto dummy_table = Table::create_dummy_table({{"a", DataType::Int, false}});
    dummy_table->add_soft_foreign_key_constraint({{ColumnID{0}}, {ColumnID{0}}, _table_a, dummy_table});

    const auto ind = InclusionDependency{{_a}, {ColumnID{0}}, dummy_table};
    EXPECT_EQ(_stored_table_node->inclusion_dependencies().size(), 1);
    EXPECT_TRUE(_stored_table_node->inclusion_dependencies().contains(ind));
  }

  EXPECT_TRUE(_stored_table_node->inclusion_dependencies().empty());
}

TEST_F(StoredTableNodeTest, HasMatchingInclusionDependency) {
  const auto table_c = Table::create_dummy_table(
      {{"a", DataType::Int, false}, {"b", DataType::Int, false}, {"c", DataType::Float, false}});
  Hyrise::get().storage_manager.add_table("t_c", table_c);

  // [t_b.a] in [t_a.a]
  const auto foreign_key_a = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{0}}, _table_a, _table_b};
  _table_b->add_soft_foreign_key_constraint(foreign_key_a);

  // [t_b.c] in [t_a.c]
  const auto foreign_key_b = ForeignKeyConstraint{{ColumnID{2}}, {ColumnID{2}}, _table_a, _table_b};
  _table_b->add_soft_foreign_key_constraint(foreign_key_b);

  // [t_c.a] in [t_a.a]
  const auto foreign_key_c = ForeignKeyConstraint{{ColumnID{0}}, {ColumnID{0}}, _table_a, table_c};
  table_c->add_soft_foreign_key_constraint(foreign_key_c);

  // [t_c.a, t_c.b] in [t_a.a, t_a.b]
  const auto foreign_key_d =
      ForeignKeyConstraint{{ColumnID{0}, ColumnID{1}}, {ColumnID{0}, ColumnID{1}}, _table_a, table_c};
  table_c->add_soft_foreign_key_constraint(foreign_key_d);

  const auto stored_table_b = StoredTableNode::make("t_b");
  const auto stored_table_c = StoredTableNode::make("t_c");

  _stored_table_node->set_pruned_column_ids({});
  EXPECT_EQ(_table_a->referenced_foreign_key_constraints().size(), 4);
  EXPECT_EQ(_stored_table_node->inclusion_dependencies().size(), 4);

  // Negative tests.
  // Columns are empty.
  EXPECT_THROW(_stored_table_node->has_matching_ind({}, *stored_table_b), std::logic_error);

  // There is no matching IND at all.
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_b}, *stored_table_b));
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_b, _c}, *stored_table_b));
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_c}, *stored_table_c));
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_a, _c}, *stored_table_c));

  // Columns are pruned on other side.
  _stored_table_node->set_pruned_column_ids({});
  stored_table_b->set_pruned_column_ids({ColumnID{0}});
  stored_table_c->set_pruned_column_ids({ColumnID{0}});
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_a}, *stored_table_b));
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_a}, *stored_table_c));
  EXPECT_FALSE(_stored_table_node->has_matching_ind({_a, _b}, *stored_table_c));

  if constexpr (HYRISE_DEBUG) {
    // Columns are not part of output_expressions() (i.e., pruned).
    _stored_table_node->set_pruned_column_ids({ColumnID{0}});
    EXPECT_THROW(_stored_table_node->has_matching_ind({_a}, *stored_table_b), std::logic_error);
  }

  // Test exact match.
  _stored_table_node->set_pruned_column_ids({});
  stored_table_b->set_pruned_column_ids({});
  stored_table_c->set_pruned_column_ids({});
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_a}, *stored_table_b));
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_c}, *stored_table_b));
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_a}, *stored_table_c));
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_a, _b}, *stored_table_c));

  // Test superset of column ids.
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_a}, *stored_table_c));
  EXPECT_TRUE(_stored_table_node->has_matching_ind({_b}, *stored_table_c));
}

}  // namespace hyrise
