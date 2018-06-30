#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/expression_factory.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

using namespace opossum::expression_factory;

namespace opossum {

class JoinNodeTest : public ::testing::Test {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node_b = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Float, "y"}}, "t_b");

    _t_a_a = {_mock_node_a, ColumnID{0}};
    _t_a_b = {_mock_node_a, ColumnID{1}};
    _t_a_c = {_mock_node_a, ColumnID{2}};
    _t_b_x = {_mock_node_b, ColumnID{0}};
    _t_b_y = {_mock_node_b, ColumnID{1}};

    _join_node = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
    _join_node->set_left_input(_mock_node_a);
    _join_node->set_right_input(_mock_node_b);

    _inner_join_node = JoinNode::make(JoinMode::Inner, equals(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _semi_join_node = JoinNode::make(JoinMode::Semi, equals(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
    _anti_join_node =  JoinNode::make(JoinMode::Anti, equals(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
  std::shared_ptr<JoinNode> _inner_join_node;
  std::shared_ptr<JoinNode> _semi_join_node;
  std::shared_ptr<JoinNode> _anti_join_node;
  std::shared_ptr<JoinNode> _join_node;
  LQPColumnReference _t_a_a;
  LQPColumnReference _t_a_b;
  LQPColumnReference _t_a_c;
  LQPColumnReference _t_b_x;
  LQPColumnReference _t_b_y;
};

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(_join_node->description(), "[Join] Mode: Cross"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(_inner_join_node->description(), "[Join] Mode: Inner a = y"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(_semi_join_node->description(), "[Join] Mode: Semi a = y"); }

TEST_F(JoinNodeTest, DescriptionAntiJoin) { EXPECT_EQ(_anti_join_node->description(), "[Join] Mode: Anti a = y"); }

TEST_F(JoinNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_join_node->column_expressions().size(), 5u);
  EXPECT_TRUE(_join_node->column_expressions().at(0)->deep_equals(*column(_t_a_a)));
  EXPECT_TRUE(_join_node->column_expressions().at(1)->deep_equals(*column(_t_a_b)));
  EXPECT_TRUE(_join_node->column_expressions().at(2)->deep_equals(*column(_t_a_c)));
  EXPECT_TRUE(_join_node->column_expressions().at(3)->deep_equals(*column(_t_b_x)));
  EXPECT_TRUE(_join_node->column_expressions().at(4)->deep_equals(*column(_t_b_y)));
}

TEST_F(JoinNodeTest, Equals) {
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_join_node, _join_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_inner_join_node, _inner_join_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_semi_join_node, _semi_join_node));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_anti_join_node, _anti_join_node));

  const auto other_join_node_a = JoinNode::make(JoinMode::Inner, equals(_t_a_a, _t_b_x), _mock_node_a, _mock_node_b);
  const auto other_join_node_b = JoinNode::make(JoinMode::Inner, not_like(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);
  const auto other_join_node_c = JoinNode::make(JoinMode::Cross, _mock_node_a, _mock_node_b);
  const auto other_join_node_d = JoinNode::make(JoinMode::Inner, equals(_t_a_a, _t_b_y), _mock_node_a, _mock_node_b);

  EXPECT_TRUE(lqp_find_subplan_mismatch(other_join_node_a, _inner_join_node).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(other_join_node_b, _inner_join_node).has_value());
  EXPECT_TRUE(lqp_find_subplan_mismatch(other_join_node_c, _inner_join_node).has_value());
  EXPECT_TRUE(!lqp_find_subplan_mismatch(other_join_node_d, _inner_join_node));
}

TEST_F(JoinNodeTest, Copy) {
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_join_node, _join_node->deep_copy()));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_inner_join_node, _inner_join_node->deep_copy()));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_semi_join_node, _semi_join_node->deep_copy()));
  EXPECT_TRUE(!lqp_find_subplan_mismatch(_anti_join_node, _anti_join_node->deep_copy()));
}

TEST_F(JoinNodeTest, OutputColumnReferencesSemiJoin) {
  ASSERT_EQ(_semi_join_node->column_expressions().size(), 3u);
  EXPECT_TRUE(_semi_join_node->column_expressions().at(0)->deep_equals(*column(_t_a_a)));
  EXPECT_TRUE(_semi_join_node->column_expressions().at(1)->deep_equals(*column(_t_a_b)));
  EXPECT_TRUE(_semi_join_node->column_expressions().at(2)->deep_equals(*column(_t_a_c)));
}

TEST_F(JoinNodeTest, OutputColumnReferencesAntiJoin) {
  ASSERT_EQ(_anti_join_node->column_expressions().size(), 3u);
  EXPECT_TRUE(_anti_join_node->column_expressions().at(0)->deep_equals(*column(_t_a_a)));
  EXPECT_TRUE(_anti_join_node->column_expressions().at(1)->deep_equals(*column(_t_a_b)));
  EXPECT_TRUE(_anti_join_node->column_expressions().at(2)->deep_equals(*column(_t_a_c)));
}

}  // namespace opossum
