#include <memory>

#include "base_test.hpp"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/delete_node.hpp"

namespace opossum {

class DeleteNodeTest : public BaseTest {
 protected:
  void SetUp() override { _delete_node = DeleteNode::make(); }

  std::shared_ptr<DeleteNode> _delete_node;
};

TEST_F(DeleteNodeTest, Description) { EXPECT_EQ(_delete_node->description(), "[Delete]"); }

TEST_F(DeleteNodeTest, Equals) {
  const auto another_delete_node = DeleteNode::make();
  EXPECT_EQ(*_delete_node, *another_delete_node);
}

TEST_F(DeleteNodeTest, NodeExpressions) { EXPECT_TRUE(_delete_node->node_expressions.empty()); }

TEST_F(DeleteNodeTest, ColumnExpressions) { EXPECT_TRUE(_delete_node->column_expressions().empty()); }

TEST_F(DeleteNodeTest, Copy) { EXPECT_EQ(*_delete_node, *_delete_node->deep_copy()); }

}  // namespace opossum
