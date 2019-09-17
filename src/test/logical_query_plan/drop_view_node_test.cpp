#include "gtest/gtest.h"

#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class DropViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override { _drop_view_node = DropViewNode::make("some_view", false); }

  std::shared_ptr<DropViewNode> _drop_view_node;
};

TEST_F(DropViewNodeTest, Description) { EXPECT_EQ(_drop_view_node->description(), "[Drop] View: 'some_view'"); }

TEST_F(DropViewNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_drop_view_node, *_drop_view_node);

  const auto same_drop_view_node = DropViewNode::make("some_view", false);
  const auto different_drop_view_node = DropViewNode::make("some_view2", false);

  EXPECT_EQ(*_drop_view_node, *same_drop_view_node);
  EXPECT_NE(*_drop_view_node, *different_drop_view_node);

  EXPECT_EQ(_drop_view_node->hash(), same_drop_view_node->hash());
  EXPECT_NE(_drop_view_node->hash(), different_drop_view_node->hash());
}

TEST_F(DropViewNodeTest, Copy) { EXPECT_EQ(*_drop_view_node->deep_copy(), *_drop_view_node); }

TEST_F(DropViewNodeTest, NodeExpressions) { ASSERT_EQ(_drop_view_node->node_expressions.size(), 0u); }

}  // namespace opossum
