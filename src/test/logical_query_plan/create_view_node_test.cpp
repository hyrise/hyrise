#include "gtest/gtest.h"

#include "logical_query_plan/create_view_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "storage/lqp_view.hpp"

namespace opossum {

class CreateViewNodeTest : public ::testing::Test {
 public:
  void SetUp() override {
    _view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
    _view = std::make_shared<LQPView>(_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, {"a"}}});
    _create_view_node = CreateViewNode::make("some_view", _view, false);
  }

  std::shared_ptr<CreateViewNode> _create_view_node;
  std::shared_ptr<LQPView> _view;
  std::shared_ptr<MockNode> _view_node;
};

TEST_F(CreateViewNodeTest, Description) {
  EXPECT_EQ(_create_view_node->description(),
            "[CreateView] Name: some_view, Columns: a FROM (\n"
            "[0] [MockNode 'Unnamed'] pruned: 0/1 columns\n"
            ")");

  const auto _create_view_node_2 = CreateViewNode::make("some_view", _view, true);
  EXPECT_EQ(_create_view_node_2->description(),
            "[CreateView] IfNotExists Name: some_view, Columns: a FROM (\n"
            "[0] [MockNode 'Unnamed'] pruned: 0/1 columns\n"
            ")");
}

TEST_F(CreateViewNodeTest, Equals) {
  EXPECT_EQ(*_create_view_node, *_create_view_node);

  const auto same_create_view_node = CreateViewNode::make("some_view", _view, false);
  const auto different_create_view_node_a = CreateViewNode::make("some_view2", _view, false);

  const auto different_view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "b"}}));
  const auto different_view =
      std::make_shared<LQPView>(different_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, {"b"}}});
  const auto different_create_view_node_b = CreateViewNode::make("some_view", different_view, false);
  const auto different_create_view_node_c = CreateViewNode::make("some_view", _view, true);

  EXPECT_NE(*different_create_view_node_a, *_create_view_node);
  EXPECT_NE(*different_create_view_node_b, *_create_view_node);
  EXPECT_NE(*different_create_view_node_c, *_create_view_node);
}

TEST_F(CreateViewNodeTest, Copy) {
  const auto same_view_node = MockNode::make(MockNode::ColumnDefinitions({{DataType::Int, "a"}}));
  const auto same_view =
      std::make_shared<LQPView>(_view_node, std::unordered_map<ColumnID, std::string>{{ColumnID{0}, "a"}});
  const auto same_create_view_node = CreateViewNode::make("some_view", _view, false);

  EXPECT_EQ(*same_create_view_node, *_create_view_node->deep_copy());
}

TEST_F(CreateViewNodeTest, NodeExpressions) { ASSERT_EQ(_view_node->node_expressions.size(), 0u); }

}  // namespace opossum
