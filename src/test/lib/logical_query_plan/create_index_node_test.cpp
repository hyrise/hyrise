#include "base_test.hpp"

#include "logical_query_plan/create_index_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/static_table_node.hpp"
#include "storage/table.hpp"
#include "storage/table_column_definition.hpp"

namespace opossum {

class CreateIndexNodeTest : public BaseTest {
 public:
  void SetUp() override {
    
    column_names.emplace_back("a", "b");
    create_index_node = CreateIndexNode::make("some_index", "some_table", false, column_names);
  }

  
  std::shared_ptr<CreateIndexNode> create_index_node;
  std::vector<std::string> column_names;
};

TEST_F(CreateIndexNodeTest, Description) {
  EXPECT_EQ(create_index_node->description(), "[CreateIndex] Name: 'some_index' Table: 'some_table'");
  auto create_index_node_2 = CreateIndexNode::make("some_index", "some_table", true, column_names);
  EXPECT_EQ(create_index_node_2->description(), "[CreateIndex] IfNotExists Name: 'some_index' Table: 'some_table");
}
TEST_F(CreateIndexNodeTest, NodeExpressions) { ASSERT_EQ(create_index_node->node_expressions.size(), 0u); }

TEST_F(CreateTableNodeTest, HashingAndEqualityCheck) {
  const auto deep_copy_node = create_index_node->deep_copy();
  EXPECT_EQ(*create_index_node, *deep_copy_node);

  const auto different_create_index_node_a = CreateTableNode::make("some_table2", false, input_node);
  const auto different_create_index_node_b = CreateTableNode::make("some_table", true, input_node);

  TableColumnDefinitions different_column_definitions;
  different_column_definitions.emplace_back("a", DataType::Int, false);
  const auto different_input_node =
      std::make_shared<StaticTableNode>(Table::create_dummy_table(different_column_definitions));
  const auto different_create_index_node_c = CreateTableNode::make("some_table", false, different_input_node);

  EXPECT_NE(*different_create_index_node_a, *create_index_node);
  EXPECT_NE(*different_create_index_node_b, *create_index_node);
  EXPECT_NE(*different_create_index_node_c, *create_index_node);

  EXPECT_NE(different_create_index_node_a->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_b->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_c->hash(), create_index_node->hash());
}

TEST_F(CreateTableNodeTest, Copy) { EXPECT_EQ(*create_index_node, *create_index_node->deep_copy()); }

}  // namespace opossum
