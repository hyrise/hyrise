#include <memory>

#include "gtest/gtest.h"

#include "expression/expression_utils.hpp"
#include "logical_query_plan/delete_node.hpp"

namespace opossum {

class DeleteNodeTest : public ::testing::Test {
 protected:
  void SetUp() override { _delete_node = DeleteNode::make("table_a"); }

  std::shared_ptr<DeleteNode> _delete_node;
};

TEST_F(DeleteNodeTest, Description) { EXPECT_EQ(_delete_node->description(), "[Delete] Table: 'table_a'"); }

TEST_F(DeleteNodeTest, Equals) {
  EXPECT_EQ(*_delete_node, *_delete_node);
  const auto different_delete_node = DeleteNode::make("table_b");
  EXPECT_NE(*_delete_node, *different_delete_node);
}

TEST_F(DeleteNodeTest, Copy) { EXPECT_EQ(*_delete_node, *_delete_node->deep_copy()); }

}  // namespace opossum
