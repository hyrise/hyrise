#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/delete_node.hpp"

namespace opossum {

class DeleteNodeTest : public BaseTest {
 protected:
  void SetUp() override { _delete_node = DeleteNode::make("table_a"); }

  std::shared_ptr<DeleteNode> _delete_node;
};

TEST_F(DeleteNodeTest, Description) { EXPECT_EQ(_delete_node->description(), "[Delete] Table: 'table_a'"); }

TEST_F(DeleteNodeTest, ShallowEquals) {
  EXPECT_TRUE(_delete_node->shallow_equals(*_delete_node));

  const auto other_delete_node = DeleteNode::make("table_b");
  EXPECT_FALSE(other_delete_node->shallow_equals(*_delete_node));
}

}  // namespace opossum
