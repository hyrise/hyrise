#include "base_test.hpp"

#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"
#include "utils/constraint_test_utils.hpp"

namespace opossum {

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
}

TEST_F(StaticTableNodeTest, NodeExpressions) { ASSERT_EQ(static_table_node->node_expressions.size(), 0u); }

TEST_F(StaticTableNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*static_table_node, *static_table_node);

  const auto same_static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));

  TableColumnDefinitions different_column_definitions;
  different_column_definitions.emplace_back("a", DataType::Int, false);

  const auto different_static_table_node =
      StaticTableNode::make(Table::create_dummy_table(different_column_definitions));

  EXPECT_EQ(*same_static_table_node, *static_table_node);
  EXPECT_NE(*different_static_table_node, *static_table_node);

  EXPECT_EQ(same_static_table_node->hash(), static_table_node->hash());
  EXPECT_NE(different_static_table_node->hash(), static_table_node->hash());
}

TEST_F(StaticTableNodeTest, Copy) { EXPECT_EQ(*static_table_node, *static_table_node->deep_copy()); }

TEST_F(StaticTableNodeTest, UniqueConstraintsEmpty) {
  EXPECT_TRUE(dummy_table->soft_key_constraints().empty());
  EXPECT_TRUE(static_table_node->unique_constraints()->empty());
}

TEST_F(StaticTableNodeTest, UniqueConstraints) {
  // Prepare two unique constraints
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  dummy_table->add_soft_key_constraint(key_constraint_a);
  dummy_table->add_soft_key_constraint(key_constraint_a_b);

  // Basic check
  const auto& unique_constraints = static_table_node->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a, unique_constraints));
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a_b, unique_constraints));
}

}  // namespace opossum
