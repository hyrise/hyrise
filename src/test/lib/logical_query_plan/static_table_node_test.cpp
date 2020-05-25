#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
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

TEST_F(StaticTableNodeTest, ConstraintsEmpty) {
  EXPECT_TRUE(dummy_table->get_soft_unique_constraints().empty());
  EXPECT_TRUE(static_table_node->constraints()->empty());
}

TEST_F(StaticTableNodeTest, Constraints) {
  // Add two constraints
  const auto constraint1 = TableConstraintDefinition{{ColumnID{0}}};
  const auto constraint2 = TableConstraintDefinition{{ColumnID{0}, ColumnID{1}}};
  dummy_table->add_soft_unique_constraint(constraint1);
  dummy_table->add_soft_unique_constraint(constraint2);
  // Verify
  check_table_constraint_representation(TableConstraintDefinitions{constraint1, constraint2},
                                        static_table_node->constraints());
}

}  // namespace opossum
