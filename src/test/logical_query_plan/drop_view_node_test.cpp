#include "gtest/gtest.h"

#include "logical_query_plan/drop_view_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class DropViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override { _drop_view_node = DropViewNode::make("some_view"); }

  std::shared_ptr<DropViewNode> _drop_view_node;
};

TEST_F(DropViewNodeTest, Description) { EXPECT_EQ(_drop_view_node->description(), "[Drop] View: 'some_view'"); }

TEST_F(DropViewNodeTest, Equals) {
  EXPECT_EQ(*_drop_view_node, *_drop_view_node);

  const auto same_drop_view_node = DropViewNode::make("some_view");
  const auto different_drop_view_node = DropViewNode::make("some_view2");

  EXPECT_EQ(*_drop_view_node, *same_drop_view_node);
  EXPECT_NE(*_drop_view_node, *different_drop_view_node);
}

TEST_F(DropViewNodeTest, Copy) { EXPECT_EQ(*_drop_view_node->deep_copy(), *_drop_view_node); }

}  // namespace opossum
