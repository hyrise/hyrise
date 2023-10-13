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

    Hyrise::get().storage_manager.add_table("t_a", _table_a);
    Hyrise::get().storage_manager.add_table("t_b",
                                            load_table("resources/test_data/tbl/int_int_float.tbl", ChunkOffset{1}));

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
  std::shared_ptr<Table> _table_a;
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
  EXPECT_EQ(_stored_table_node->output_expressions().size(), 3);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(0), *_a);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(1), *_b);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(2), *_c);

  // Column pruning does not interfere with get_column()
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});
  EXPECT_EQ(_stored_table_node->output_expressions().size(), 2);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(0), *_b);
  EXPECT_EQ(*_stored_table_node->output_expressions().at(1), *_c);
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

TEST_F(StoredTableNodeTest, HashingAndEqualityWithPrunableSubqueryPredicates) {
  const auto different_node_a = std::static_pointer_cast<StoredTableNode>(_stored_table_node->deep_copy());
  const auto different_node_b = std::static_pointer_cast<StoredTableNode>(_stored_table_node->deep_copy());
  const auto different_node_c = std::static_pointer_cast<StoredTableNode>(_stored_table_node->deep_copy());
  const auto different_node_d = std::static_pointer_cast<StoredTableNode>(_stored_table_node->deep_copy());

  const auto stored_table_node_b = StoredTableNode::make("t_b");
  const auto stored_table_node_b_col_a = stored_table_node_b->get_column("a");

  // clang-format off
  const auto subquery_a =
  ProjectionNode::make(expression_vector(min_(stored_table_node_b_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(min_(stored_table_node_b_col_a)),
      stored_table_node_b));

  const auto lqp_a =
  PredicateNode::make(equals_(different_node_a->get_column("a"), lqp_subquery_(subquery_a)),
    different_node_a);

  // Different predicate column than lqp_a.
  const auto lqp_b =
  PredicateNode::make(equals_(different_node_b->get_column("b"), lqp_subquery_(subquery_a)),
    different_node_b);

  // Different predicate condition than lqp_a.
  const auto lqp_c =
  PredicateNode::make(less_than_(different_node_c->get_column("a"), lqp_subquery_(subquery_a)),
    different_node_c);

  const auto subquery_d =
  ProjectionNode::make(expression_vector(max_(stored_table_node_b_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(max_(stored_table_node_b_col_a)),
      stored_table_node_b));

  // Different subquery than lqp_a.
  const auto lqp_d =
  PredicateNode::make(equals_(different_node_d->get_column("a"), lqp_subquery_(subquery_d)),
    different_node_d);
  // clang-format on

  different_node_a->set_prunable_subquery_predicates({lqp_a});
  different_node_b->set_prunable_subquery_predicates({lqp_b});
  different_node_c->set_prunable_subquery_predicates({lqp_c});
  different_node_d->set_prunable_subquery_predicates({lqp_d});

  EXPECT_NE(*_stored_table_node, *different_node_a);
  EXPECT_NE(*_stored_table_node, *different_node_b);
  EXPECT_NE(*_stored_table_node, *different_node_c);
  EXPECT_NE(*_stored_table_node, *different_node_d);
  EXPECT_NE(*different_node_a, *different_node_b);
  EXPECT_NE(*different_node_a, *different_node_c);
  EXPECT_NE(*different_node_a, *different_node_d);
  EXPECT_NE(*different_node_b, *different_node_c);
  EXPECT_NE(*different_node_b, *different_node_d);
  EXPECT_NE(*different_node_c, *different_node_d);

  EXPECT_NE(_stored_table_node->hash(), different_node_a->hash());
  EXPECT_NE(_stored_table_node->hash(), different_node_b->hash());
  EXPECT_NE(_stored_table_node->hash(), different_node_c->hash());
  EXPECT_NE(_stored_table_node->hash(), different_node_d->hash());

  // We force hash collisions for nodes with the same number of prunable subquery predicates.
  EXPECT_EQ(different_node_a->hash(), different_node_b->hash());
  EXPECT_EQ(different_node_a->hash(), different_node_c->hash());
  EXPECT_EQ(different_node_a->hash(), different_node_d->hash());
}

TEST_F(StoredTableNodeTest, Copy) {
  EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node);

  _stored_table_node->set_pruned_chunk_ids({ChunkID{2}});
  _stored_table_node->set_pruned_column_ids({ColumnID{1}});
  EXPECT_EQ(*_stored_table_node->deep_copy(), *_stored_table_node);

  const auto stored_table_node_b = StoredTableNode::make("t_b");
  const auto stored_table_node_b_col_a = stored_table_node_b->get_column("a");

  // clang-format off
  const auto subquery =
  ProjectionNode::make(expression_vector(min_(stored_table_node_b_col_a)),
    AggregateNode::make(expression_vector(), expression_vector(min_(stored_table_node_b_col_a)),
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
  table->add_soft_key_constraint({{_b->original_column_id}, KeyConstraintType::UNIQUE});

  const auto& fds = _stored_table_node->functional_dependencies();

  const auto fd1_expected = FunctionalDependency{{_a}, {_b, _c}};
  const auto fd2_expected = FunctionalDependency{{_b}, {_a, _c}};

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
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _table_a->add_soft_key_constraint(key_constraint_a_b);
  _table_a->add_soft_key_constraint(key_constraint_c);
  const auto& table_key_constraints = _table_a->soft_key_constraints();
  EXPECT_EQ(table_key_constraints.size(), 2);
  EXPECT_EQ(_stored_table_node->unique_column_combinations().size(), 2);

  // Prune column a, which should remove one UCC.
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
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_b}));
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_c}));
  EXPECT_FALSE(_stored_table_node->has_matching_ucc({_b, _c}));

  // Test exact match.
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a}));

  // Test superset of column ids.
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a, _b}));
  EXPECT_TRUE(_stored_table_node->has_matching_ucc({_a, _c}));
}

}  // namespace hyrise
