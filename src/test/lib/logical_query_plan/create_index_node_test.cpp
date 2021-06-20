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
    column_names.emplace_back("a");
    column_names.emplace_back("b");
    create_index_node = CreateIndexNode::make("some_index", "some_table", false, column_names);
  }

  std::shared_ptr<CreateIndexNode> create_index_node;
  std::vector<std::string> column_names;
};

TEST_F(CreateIndexNodeTest, Description) {
  EXPECT_EQ(create_index_node->description(), "[CreateIndex] Name: 'some_index' Table: 'some_table'");
  auto create_index_node_2 = CreateIndexNode::make("some_index2", "some_table2", true, column_names);
  EXPECT_EQ(create_index_node_2->description(), "[CreateIndex] IfNotExists Name: 'some_index2' Table: 'some_table2'");
}
TEST_F(CreateIndexNodeTest, NodeExpressions) { ASSERT_EQ(create_index_node->node_expressions.size(), 0u); }

TEST_F(CreateIndexNodeTest, HashingAndEqualityCheck) {
  const auto deep_copy_node = create_index_node->deep_copy();
  EXPECT_EQ(*create_index_node, *deep_copy_node);

  const auto different_create_index_node_a = CreateIndexNode::make("some_index1", "some_table", false, column_names);
  const auto different_create_index_node_b = CreateIndexNode::make("some_index", "some_table1", false, column_names);
  const auto different_create_index_node_c = CreateIndexNode::make("some_index", "some_table", true, column_names);

  auto different_column_names = std::vector<std::string>();
  different_column_names.push_back("c");
  different_column_names.push_back("d");
  const auto different_create_index_node_d = CreateIndexNode::make("some_index", "some_table", false, different_column_names);

  EXPECT_NE(*different_create_index_node_a, *create_index_node);
  EXPECT_NE(*different_create_index_node_b, *create_index_node);
  EXPECT_NE(*different_create_index_node_c, *create_index_node);
  EXPECT_NE(*different_create_index_node_d, *create_index_node);

  EXPECT_NE(different_create_index_node_a->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_b->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_c->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_d->hash(), create_index_node->hash());
}

TEST_F(CreateIndexNodeTest, Copy) { EXPECT_EQ(*create_index_node, *create_index_node->deep_copy()); }

}  // namespace opossum
