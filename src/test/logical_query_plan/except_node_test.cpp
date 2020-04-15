#include <memory>

#include "base_test.hpp"

#include "logical_query_plan/except_node.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"

namespace opossum {

class ExceptNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node1 = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");
    _mock_node3 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}}, "t_v");

    _a = {_mock_node1, ColumnID{0}};
    _b = {_mock_node1, ColumnID{1}};
    _c = {_mock_node1, ColumnID{2}};

    _except_node = ExceptNode::make(SetOperationMode::Positions);
    _except_node->set_left_input(_mock_node1);
    _except_node->set_right_input(_mock_node1);
  }

  std::shared_ptr<MockNode> _mock_node1, _mock_node2, _mock_node3;
  std::shared_ptr<ExceptNode> _except_node;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
};

TEST_F(ExceptNodeTest, Description) { EXPECT_EQ(_except_node->description(), "[ExceptNode] Mode: Positions"); }

TEST_F(ExceptNodeTest, OutputColumnExpressions) {
  EXPECT_TRUE(_except_node->column_expressions() == _mock_node1->column_expressions());
}

TEST_F(ExceptNodeTest, HashingAndEqualityCheck) {
  auto same_except_node = ExceptNode::make(SetOperationMode::Positions, _mock_node1, _mock_node1);
  auto different_except_node = ExceptNode::make(SetOperationMode::All, _mock_node1, _mock_node1);
  auto different_except_node_1 = ExceptNode::make(SetOperationMode::Positions, _mock_node1, _mock_node2);
  auto different_except_node_2 = ExceptNode::make(SetOperationMode::Positions, _mock_node2, _mock_node1);
  auto different_except_node_3 = ExceptNode::make(SetOperationMode::Positions, _mock_node2, _mock_node2);

  EXPECT_EQ(*_except_node, *same_except_node);
  EXPECT_NE(*_except_node, *different_except_node);
  EXPECT_NE(*_except_node, *different_except_node_1);
  EXPECT_NE(*_except_node, *different_except_node_2);
  EXPECT_NE(*_except_node, *different_except_node_3);
  EXPECT_NE(*_except_node, *ExceptNode::make(SetOperationMode::Positions));
  EXPECT_NE(*_except_node, *ExceptNode::make(SetOperationMode::All));

  EXPECT_EQ(_except_node->hash(), same_except_node->hash());
  EXPECT_NE(_except_node->hash(), different_except_node->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_1->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_2->hash());
  EXPECT_NE(_except_node->hash(), different_except_node_3->hash());
}

TEST_F(ExceptNodeTest, Copy) { EXPECT_EQ(*_except_node->deep_copy(), *_except_node); }

TEST_F(ExceptNodeTest, NodeExpressions) { ASSERT_EQ(_except_node->node_expressions.size(), 0u); }

}  // namespace opossum
