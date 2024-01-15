#include "base_test.hpp"

#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

class StaticTableNodeTest : public BaseTest {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);
    dummy_table = Table::create_dummy_table(column_definitions);

    static_table_node = StaticTableNode::make(dummy_table);
  }

  std::shared_ptr<Table> dummy_table;
  TableColumnDefinitions column_definitions;
  std::shared_ptr<StaticTableNode> static_table_node;
};

TEST_F(StaticTableNodeTest, Description) {
  EXPECT_EQ(static_table_node->description(), "[StaticTable]: (a int not nullable, b float nullable)");

  dummy_table->add_soft_key_constraint({{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY});
  dummy_table->add_soft_key_constraint({{ColumnID{1}}, KeyConstraintType::UNIQUE});

  EXPECT_EQ(static_table_node->description(),
            "[StaticTable]: (a int not nullable, b float nullable, PRIMARY_KEY(a), UNIQUE(b))");
}

TEST_F(StaticTableNodeTest, NodeExpressions) {
  ASSERT_EQ(static_table_node->node_expressions.size(), 0u);
}

TEST_F(StaticTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*static_table_node, *static_table_node);

  const auto same_static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));

  TableColumnDefinitions different_column_definitions;
  different_column_definitions.emplace_back("a", DataType::Int, false);

  const auto different_static_table_node_by_definitions =
      StaticTableNode::make(Table::create_dummy_table(different_column_definitions));

  const auto different_static_table_node_by_constraints =
      StaticTableNode::make(Table::create_dummy_table(column_definitions));
  different_static_table_node_by_constraints->table->add_soft_key_constraint(
      {{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY});

  EXPECT_EQ(*same_static_table_node, *static_table_node);
  EXPECT_NE(*different_static_table_node_by_definitions, *static_table_node);
  EXPECT_NE(*different_static_table_node_by_constraints, *static_table_node);

  EXPECT_EQ(same_static_table_node->hash(), static_table_node->hash());
  EXPECT_NE(different_static_table_node_by_definitions->hash(), static_table_node->hash());
  EXPECT_NE(different_static_table_node_by_constraints->hash(), static_table_node->hash());
}

TEST_F(StaticTableNodeTest, HashingAndEqualityConstraintOrder) {
  const auto key_constraint_1 = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_2 = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto same_static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));

  static_table_node->table->add_soft_key_constraint(key_constraint_1);
  static_table_node->table->add_soft_key_constraint(key_constraint_2);

  same_static_table_node->table->add_soft_key_constraint(key_constraint_2);
  same_static_table_node->table->add_soft_key_constraint(key_constraint_1);

  EXPECT_EQ(*same_static_table_node, *static_table_node);
  EXPECT_EQ(same_static_table_node->hash(), static_table_node->hash());
}

TEST_F(StaticTableNodeTest, Copy) {
  EXPECT_EQ(*static_table_node, *static_table_node->deep_copy());
}

TEST_F(StaticTableNodeTest, UniqueColumnCombinationsEmpty) {
  EXPECT_TRUE(dummy_table->soft_key_constraints().empty());
  EXPECT_TRUE(static_table_node->unique_column_combinations().empty());
}

TEST_F(StaticTableNodeTest, UniqueColumnCombinations) {
  // Prepare two unique constraints.
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_b = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  dummy_table->add_soft_key_constraint(key_constraint_a);
  dummy_table->add_soft_key_constraint(key_constraint_b);

  // Basic check.
  const auto& unique_column_combinations = static_table_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 2);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a, unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_b, unique_column_combinations));
}

}  // namespace hyrise
