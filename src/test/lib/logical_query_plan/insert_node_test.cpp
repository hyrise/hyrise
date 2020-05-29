#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/insert_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"

namespace opossum {

class InsertNodeTest : public BaseTest {
 protected:
  void SetUp() override { _insert_node = InsertNode::make("table_a"); }

  std::shared_ptr<InsertNode> _insert_node;
};

TEST_F(InsertNodeTest, Description) { EXPECT_EQ(_insert_node->description(), "[Insert] Into table 'table_a'"); }

TEST_F(InsertNodeTest, TableName) { EXPECT_EQ(_insert_node->table_name, "table_a"); }

TEST_F(InsertNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_insert_node, *_insert_node);
  EXPECT_EQ(*_insert_node, *InsertNode::make("table_a"));
  EXPECT_NE(*_insert_node, *InsertNode::make("table_b"));

  EXPECT_EQ(_insert_node->hash(), InsertNode::make("table_a")->hash());
  EXPECT_NE(_insert_node->hash(), InsertNode::make("table_b")->hash());
}

TEST_F(InsertNodeTest, NodeExpressions) { EXPECT_TRUE(_insert_node->node_expressions.empty()); }

TEST_F(InsertNodeTest, ColumnExpressions) { EXPECT_TRUE(_insert_node->column_expressions().empty()); }

}  // namespace opossum
