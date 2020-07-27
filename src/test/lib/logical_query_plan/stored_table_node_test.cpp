#include <memory>
#include <string>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "hyrise.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/index/group_key/composite_group_key_index.hpp"
#include "storage/index/group_key/group_key_index.hpp"
#include "storage/table_key_constraint.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class StoredTableNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    Hyrise::reset();

    Hyrise::get().storage_manager.add_table("t_a", load_table("resources/test_data/tbl/int_int_float.tbl", 1));
    Hyrise::get().storage_manager.add_table("t_b", load_table("resources/test_data/tbl/int_int_float.tbl", 1));

    const auto& table_t_a = Hyrise::get().storage_manager.get_table("t_a");
    ChunkEncoder::encode_all_chunks(table_t_a);
    table_t_a->create_index<GroupKeyIndex>({ColumnID{0}}, "i_a1");
    table_t_a->create_index<GroupKeyIndex>({ColumnID{1}}, "i_b");
    table_t_a->create_index<CompositeGroupKeyIndex>({ColumnID{0}, ColumnID{1}}, "i_a2");
    table_t_a->create_index<CompositeGroupKeyIndex>({ColumnID{1}, ColumnID{0}}, "i_a3");

    _stored_table_node = StoredTableNode::make("t_a");
    _a = _stored_table_node->get_column("a");
    _b = _stored_table_node->get_column("b");
    _c = _stored_table_node->get_column("c");

    _stored_table_node->set_pruned_chunk_ids({ChunkID{2}});
  }

  std::shared_ptr<StoredTableNode> _stored_table_node;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c;
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
}

TEST_F(StoredTableNodeTest, NodeExpressions) { ASSERT_EQ(_stored_table_node->node_expressions.size(), 0u); }

TEST_F(StoredTableNodeTest, GetStatisticsPruneFirstColumn) {
  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 4u);

  auto expected_statistics = _stored_table_node->indexes_statistics().at(1u);

  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  // column with ColumnID{0} was pruned, therefore the column has to be left shifted
  expected_statistics.column_ids[0] -= 1;

  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 1u);
  EXPECT_EQ(_stored_table_node->indexes_statistics().at(0u), expected_statistics);
}

TEST_F(StoredTableNodeTest, GetStatisticsPruneSecondColumn) {
  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 4u);

  auto expected_statistics = _stored_table_node->indexes_statistics().at(0u);

  _stored_table_node->set_pruned_column_ids({ColumnID{1}});

  // column with ColumnID{1} was pruned, so ColumnID{0} should be untouched

  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 1u);
  EXPECT_EQ(_stored_table_node->indexes_statistics().at(0u), expected_statistics);
}

TEST_F(StoredTableNodeTest, GetStatisticsPruneBothColumns) {
  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 4u);

  _stored_table_node->set_pruned_column_ids({ColumnID{0}, ColumnID{1}});

  // All indexed columns were pruned, therefore the index statistics should be empty
  EXPECT_EQ(_stored_table_node->indexes_statistics().size(), 0u);
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
  EXPECT_EQ(fds.at(0), fd_expected);
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
  EXPECT_EQ(_stored_table_node->functional_dependencies().at(0), fd_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesPrunedRightColumnSet) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});

  // Prune column "b", which would be part of the right column set in the resulting FD: {a} => {b, c}
  _stored_table_node->set_pruned_column_ids({ColumnID{1}});

  const auto fd_expected = FunctionalDependency{{_a}, {_c}};
  EXPECT_EQ(_stored_table_node->functional_dependencies().size(), 1);
  EXPECT_EQ(_stored_table_node->functional_dependencies().at(0), fd_expected);
}

TEST_F(StoredTableNodeTest, FunctionalDependenciesMultiple) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");  // int_int_float.tbl
  table->add_soft_key_constraint({{_a->original_column_id}, KeyConstraintType::UNIQUE});
  table->add_soft_key_constraint({{_a->original_column_id, _b->original_column_id}, KeyConstraintType::UNIQUE});

  const auto& fds = _stored_table_node->functional_dependencies();

  const auto fd1_expected = FunctionalDependency{{_a}, {_b, _c}};
  const auto fd2_expected = FunctionalDependency{{_a, _b}, {_c}};

  EXPECT_EQ(fds.size(), 2);
  EXPECT_EQ(fds.at(0), fd1_expected);
  EXPECT_EQ(fds.at(1), fd2_expected);
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
    EXPECT_EQ(fds.at(0), fd_expected);
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
    EXPECT_EQ(fds.at(0), fd_expected);
  }

  // Test {b} => {a, c}
  {
    const auto table = std::make_shared<Table>(column_definitions, TableType::Data);
    table->add_soft_key_constraint({{ColumnID{1}}, KeyConstraintType::UNIQUE});

    Hyrise::get().storage_manager.add_table("table_d", table);
    const auto& stored_table_node = StoredTableNode::make("table_d");

    EXPECT_EQ(stored_table_node->functional_dependencies().size(), 0);
  }
}

TEST_F(StoredTableNodeTest, UniqueConstraints) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");

  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  table->add_soft_key_constraint(key_constraint_a_b);
  table->add_soft_key_constraint(key_constraint_c);

  const auto& unique_constraints = _stored_table_node->unique_constraints();

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a_b, unique_constraints));
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints));

  // Check whether StoredTableNode is referenced by the constraint's expressions
  for (const auto& unique_constraint : *unique_constraints) {
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node.expired());
      EXPECT_TRUE(column_expression->original_node.lock() == _stored_table_node);
    }
  }
}

TEST_F(StoredTableNodeTest, UniqueConstraintsPrunedColumns) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");

  // Prepare unique constraints
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  table->add_soft_key_constraint(key_constraint_a);
  table->add_soft_key_constraint(key_constraint_a_b);
  table->add_soft_key_constraint(key_constraint_c);
  const auto& table_key_constraints = table->soft_key_constraints();
  EXPECT_EQ(table_key_constraints.size(), 3);
  EXPECT_EQ(_stored_table_node->unique_constraints()->size(), 3);

  // Prune column a, which should remove two unique constraints
  _stored_table_node->set_pruned_column_ids({ColumnID{0}});

  // Basic check
  const auto& unique_constraints = _stored_table_node->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints));
}

TEST_F(StoredTableNodeTest, UniqueConstraintsEmpty) {
  EXPECT_TRUE(Hyrise::get().storage_manager.get_table(_stored_table_node->table_name)->soft_key_constraints().empty());
  EXPECT_TRUE(_stored_table_node->unique_constraints()->empty());
}

TEST_F(StoredTableNodeTest, HasMatchingUniqueConstraint) {
  const auto table = Hyrise::get().storage_manager.get_table("t_a");
  const auto key_constraint_a = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  table->add_soft_key_constraint(key_constraint_a);
  EXPECT_EQ(_stored_table_node->unique_constraints()->size(), 1);

  // Negative test
  EXPECT_FALSE(_stored_table_node->has_matching_unique_constraint({_b}));
  EXPECT_FALSE(_stored_table_node->has_matching_unique_constraint({_c}));
  EXPECT_FALSE(_stored_table_node->has_matching_unique_constraint({_b, _c}));

  // Test exact match
  EXPECT_TRUE(_stored_table_node->has_matching_unique_constraint({_a}));

  // Test superset of column ids
  EXPECT_TRUE(_stored_table_node->has_matching_unique_constraint({_a, _b}));
  EXPECT_TRUE(_stored_table_node->has_matching_unique_constraint({_a, _c}));
}

}  // namespace opossum
