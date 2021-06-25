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
    Hyrise::get().storage_manager.add_table("t_a", load_table("resources/test_data/tbl/int_int_float.tbl", 1));
    table_node = std::make_shared<StoredTableNode>("t_a");
    column_ids->emplace_back(1);
    column_ids->emplace_back(2);
    create_index_node = CreateIndexNode::make("some_index", false, column_ids, table_node);
  }

  std::shared_ptr<AbstractLQPNode> table_node;
  std::shared_ptr<CreateIndexNode> create_index_node;
  std::shared_ptr<std::vector<ColumnID>> column_ids;

};

TEST_F(CreateIndexNodeTest, Description) {
  EXPECT_EQ(create_index_node->description(), "[CreateIndex] Name: 'some_index'");
  auto create_index_node_2 = CreateIndexNode::make("some_index2", true, column_ids, table_node);
  EXPECT_EQ(create_index_node_2->description(), "[CreateIndex] IfNotExists Name: 'some_index2'");
}
TEST_F(CreateIndexNodeTest, NodeExpressions) { ASSERT_EQ(create_index_node->node_expressions.size(), 0u); }

TEST_F(CreateIndexNodeTest, HashingAndEqualityCheck) {
  const auto deep_copy_node = create_index_node->deep_copy();
  EXPECT_EQ(*create_index_node, *deep_copy_node);

  const auto different_create_index_node_a = CreateIndexNode::make("some_index1", false, column_ids, table_node);
  const auto different_create_index_node_b = CreateIndexNode::make("some_index",  true, column_ids, table_node);

  auto different_column_ids = std::shared_ptr<std::vector<ColumnID>>();
  different_column_ids->emplace_back(0);
  different_column_ids->emplace_back(3);
  const auto different_create_index_node_c = CreateIndexNode::make("some_index", false, different_column_ids, table_node);

  EXPECT_NE(*different_create_index_node_a, *create_index_node);
  EXPECT_NE(*different_create_index_node_b, *create_index_node);
  EXPECT_NE(*different_create_index_node_c, *create_index_node);


  EXPECT_NE(different_create_index_node_a->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_b->hash(), create_index_node->hash());
  EXPECT_NE(different_create_index_node_c->hash(), create_index_node->hash());

}

TEST_F(CreateIndexNodeTest, Copy) { EXPECT_EQ(*create_index_node, *create_index_node->deep_copy()); }

}  // namespace opossum
