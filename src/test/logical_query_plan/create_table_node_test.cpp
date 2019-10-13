#include "gtest/gtest.h"

#include "logical_query_plan/create_table_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class CreateTableNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    column_definitions.emplace_back("a", DataType::Int, false);
    column_definitions.emplace_back("b", DataType::Float, true);
    input_node = std::make_shared<StaticTableNode>(Table::create_dummy_table(column_definitions));
    create_table_node = CreateTableNode::make("some_table", false, input_node);
  }

  TableColumnDefinitions column_definitions;
  std::shared_ptr<StaticTableNode> input_node;
  std::shared_ptr<CreateTableNode> create_table_node;
};

TEST_F(CreateTableNodeTest, Description) {
  EXPECT_EQ(create_table_node->description(), "[CreateTable] Name: 'some_table'");
  auto create_table_node_2 = CreateTableNode::make("some_table", true, input_node);
  EXPECT_EQ(create_table_node_2->description(), "[CreateTable] IfNotExists Name: 'some_table'");
}

TEST_F(CreateTableNodeTest, NodeExpressions) { ASSERT_EQ(create_table_node->node_expressions.size(), 0u); }

TEST_F(CreateTableNodeTest, HashingAndEqualityCheck) {
  const auto deep_copy_node = create_table_node->deep_copy();
  EXPECT_EQ(*create_table_node, *deep_copy_node);

  const auto different_create_table_node_a = CreateTableNode::make("some_table2", false, input_node);
  const auto different_create_table_node_b = CreateTableNode::make("some_table", true, input_node);

  TableColumnDefinitions different_column_definitions;
  different_column_definitions.emplace_back("a", DataType::Int, false);
  const auto different_input_node =
      std::make_shared<StaticTableNode>(Table::create_dummy_table(different_column_definitions));
  const auto different_create_table_node_c = CreateTableNode::make("some_table", false, different_input_node);

  EXPECT_NE(*different_create_table_node_a, *create_table_node);
  EXPECT_NE(*different_create_table_node_b, *create_table_node);
  EXPECT_NE(*different_create_table_node_c, *create_table_node);

  EXPECT_NE(different_create_table_node_a->hash(), create_table_node->hash());
  EXPECT_NE(different_create_table_node_b->hash(), create_table_node->hash());
  EXPECT_NE(different_create_table_node_c->hash(), create_table_node->hash());
}

TEST_F(CreateTableNodeTest, Copy) { EXPECT_EQ(*create_table_node, *create_table_node->deep_copy()); }

}  // namespace opossum
