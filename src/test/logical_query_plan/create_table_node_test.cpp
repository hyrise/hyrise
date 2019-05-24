#include "gtest/gtest.h"

#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class CreateTableNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);
    create_table_node = CreateTableNode::make("some_table", column_definitions, false);
  }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<CreateTableNode> create_table_node;
};

TEST_F(CreateTableNodeTest, Description) {
  EXPECT_EQ(create_table_node->description(), "[CreateTable] Name: 'some_table' ('a' int NOT NULL, 'b' float NULL)");
  auto create_table_node_2 = CreateTableNode::make("some_table", column_definitions, true);
  EXPECT_EQ(create_table_node_2->description(),
            "[CreateTable] IfNotExists Name: 'some_table' ('a' int NOT NULL, 'b' float NULL)");
}

TEST_F(CreateTableNodeTest, NodeExpressions) { ASSERT_EQ(create_table_node->node_expressions.size(), 0u); }

TEST_F(CreateTableNodeTest, Equals) {
  EXPECT_EQ(*create_table_node, *create_table_node);

  const auto same_create_table_node = CreateTableNode::make("some_table", column_definitions, false);
  const auto different_create_table_node_a = CreateTableNode::make("some_table2", column_definitions, false);
  const auto different_create_table_node_b = CreateTableNode::make("some_table", column_definitions, true);

  TableColumnDefinitions different_column_definitions;
  column_definitions.emplace_back("a", DataType::Int, false);
  const auto different_create_table_node_c = CreateTableNode::make("some_table", column_definitions, false);

  EXPECT_NE(*different_create_table_node_a, *create_table_node);
  EXPECT_NE(*different_create_table_node_b, *create_table_node);
  EXPECT_NE(*different_create_table_node_c, *create_table_node);
}

TEST_F(CreateTableNodeTest, Copy) { EXPECT_EQ(*create_table_node, *create_table_node->deep_copy()); }

}  // namespace opossum
