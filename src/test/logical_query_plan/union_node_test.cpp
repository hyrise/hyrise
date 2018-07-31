#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

class UnionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node1 = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node2 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "u"}, {DataType::Int, "v"}}, "t_b");
    _mock_node3 = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}}, "t_v");

    _a = {_mock_node1, ColumnID{0}};
    _b = {_mock_node1, ColumnID{1}};
    _c = {_mock_node1, ColumnID{2}};

    _union_node = UnionNode::make(UnionMode::Positions);
    _union_node->set_left_input(_mock_node1);
    _union_node->set_right_input(_mock_node1);
  }

  std::shared_ptr<MockNode> _mock_node1, _mock_node2, _mock_node3;
  std::shared_ptr<UnionNode> _union_node;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
};

TEST_F(UnionNodeTest, Description) { EXPECT_EQ(_union_node->description(), "[UnionNode] Mode: UnionPositions"); }

TEST_F(UnionNodeTest, StatisticsNotImplemented) {
  EXPECT_THROW(_union_node->derive_statistics_from(_mock_node1, _mock_node1), std::exception);
}

TEST_F(UnionNodeTest, OutputColumnExpressions) {
  EXPECT_EQ(*_union_node->column_expressions().at(0), *_mock_node1->column_expressions().at(0));
  EXPECT_EQ(*_union_node->column_expressions().at(1), *_mock_node1->column_expressions().at(1));
  EXPECT_EQ(*_union_node->column_expressions().at(2), *_mock_node1->column_expressions().at(2));
}

TEST_F(UnionNodeTest, Equals) { EXPECT_EQ(*_union_node, *_union_node); }

TEST_F(UnionNodeTest, Copy) { EXPECT_EQ(*_union_node->deep_copy(), *_union_node); }

}  // namespace opossum
