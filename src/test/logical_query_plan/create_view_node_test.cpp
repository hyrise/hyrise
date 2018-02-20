#include "gtest/gtest.h"

#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class CreateViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    _view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _create_view_node = std::make_shared<CreateViewNode>("some_view", _view_node);
  }

  std::shared_ptr<CreateViewNode> _create_view_node;
  std::shared_ptr<MockNode> _view_node;
};

TEST_F(CreateViewNodeTest, ShallowEquals) {
  EXPECT_TRUE(_create_view_node->shallow_equals(*_create_view_node));

  const auto other_view_node_a = std::make_shared<CreateViewNode>("some_view", _view_node);
  EXPECT_TRUE(other_view_node_a->shallow_equals(*_create_view_node));
  EXPECT_TRUE(_create_view_node->shallow_equals(*other_view_node_a));

  const auto other_view_node_b = std::make_shared<CreateViewNode>("some_views", _view_node);
  EXPECT_FALSE(other_view_node_b->shallow_equals(*_create_view_node));
}

}  // namespace opossum
