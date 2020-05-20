#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class StaticTableNodeTest : public BaseTest {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);
    static_table_node = StaticTableNode::make(Table::create_dummy_table(column_definitions));
  }

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

}  // namespace opossum
