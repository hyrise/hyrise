#include <memory>
#include <utility>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class JoinNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node_a = std::make_shared<MockNode>(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");
    _mock_node_b =
        std::make_shared<MockNode>(MockNode::ColumnDefinitions{{DataType::Int, "x"}, {DataType::Float, "y"}}, "t_b");

    _t_a_a = {_mock_node_a, ColumnID{0}};
    _t_a_b = {_mock_node_a, ColumnID{1}};
    _t_a_c = {_mock_node_a, ColumnID{2}};
    _t_b_x = {_mock_node_b, ColumnID{0}};
    _t_b_y = {_mock_node_b, ColumnID{1}};

    _join_node = std::make_shared<JoinNode>(JoinMode::Cross);
    _join_node->set_left_child(_mock_node_a);
    _join_node->set_right_child(_mock_node_b);

    _inner_join_node =
        std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(_t_a_a, _t_b_y), PredicateCondition::Equals);
    _inner_join_node->set_left_child(_mock_node_a);
    _inner_join_node->set_right_child(_mock_node_b);

    _semi_join_node =
        std::make_shared<JoinNode>(JoinMode::Semi, std::make_pair(_t_a_a, _t_b_y), PredicateCondition::Equals);
    _semi_join_node->set_left_child(_mock_node_a);
    _semi_join_node->set_right_child(_mock_node_b);

    _anti_join_node =
        std::make_shared<JoinNode>(JoinMode::Anti, std::make_pair(_t_a_a, _t_b_y), PredicateCondition::Equals);
    _anti_join_node->set_left_child(_mock_node_a);
    _anti_join_node->set_right_child(_mock_node_b);
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

TEST_F(JoinNodeTest, Description) { EXPECT_EQ(_join_node->description(), "[Cross Join]"); }

TEST_F(JoinNodeTest, DescriptionInnerJoin) { EXPECT_EQ(_inner_join_node->description(), "[Inner Join] t_a.a = t_b.y"); }

TEST_F(JoinNodeTest, DescriptionSemiJoin) { EXPECT_EQ(_semi_join_node->description(), "[Semi Join] t_a.a = t_b.y"); }

TEST_F(JoinNodeTest, DescriptionAntiJoin) { EXPECT_EQ(_anti_join_node->description(), "[Anti Join] t_a.a = t_b.y"); }

TEST_F(JoinNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_join_node->get_verbose_column_name(ColumnID{0}), "t_a.a");
  EXPECT_EQ(_join_node->get_verbose_column_name(ColumnID{1}), "t_a.b");
  EXPECT_EQ(_join_node->get_verbose_column_name(ColumnID{2}), "t_a.c");
  EXPECT_EQ(_join_node->get_verbose_column_name(ColumnID{3}), "t_b.x");
  EXPECT_EQ(_join_node->get_verbose_column_name(ColumnID{4}), "t_b.y");
}

TEST_F(JoinNodeTest, ColumnReferenceByNamedColumnReference) {
  EXPECT_EQ(_join_node->get_column({"a", std::nullopt}), _t_a_a);
  EXPECT_EQ(_join_node->get_column({"a", "t_a"}), _t_a_a);
  EXPECT_EQ(_join_node->get_column({"b", std::nullopt}), _t_a_b);
  EXPECT_EQ(_join_node->get_column({"b", "t_a"}), _t_a_b);
  EXPECT_EQ(_join_node->get_column({"c", std::nullopt}), _t_a_c);
  EXPECT_EQ(_join_node->get_column({"c", "t_a"}), _t_a_c);
  EXPECT_EQ(_join_node->get_column({"x", std::nullopt}), _t_b_x);
  EXPECT_EQ(_join_node->get_column({"x", "t_b"}), _t_b_x);
  EXPECT_EQ(_join_node->get_column({"y", std::nullopt}), _t_b_y);
  EXPECT_EQ(_join_node->get_column({"y", "t_b"}), _t_b_y);
}

TEST_F(JoinNodeTest, OutputColumnReferences) {
  ASSERT_EQ(_join_node->output_column_references().size(), 5u);
  EXPECT_EQ(_join_node->output_column_references().at(0), _t_a_a);
  EXPECT_EQ(_join_node->output_column_references().at(1), _t_a_b);
  EXPECT_EQ(_join_node->output_column_references().at(2), _t_a_c);
  EXPECT_EQ(_join_node->output_column_references().at(3), _t_b_x);
  EXPECT_EQ(_join_node->output_column_references().at(4), _t_b_y);
}

TEST_F(JoinNodeTest, OutputColumnReferencesSemiJoin) {
  ASSERT_EQ(_semi_join_node->output_column_references().size(), 3u);
  EXPECT_EQ(_semi_join_node->output_column_references().at(0), _t_a_a);
  EXPECT_EQ(_semi_join_node->output_column_references().at(1), _t_a_b);
  EXPECT_EQ(_semi_join_node->output_column_references().at(2), _t_a_c);
}

TEST_F(JoinNodeTest, OutputColumnReferencesAntiJoin) {
  ASSERT_EQ(_anti_join_node->output_column_references().size(), 3u);
  EXPECT_EQ(_anti_join_node->output_column_references().at(0), _t_a_a);
  EXPECT_EQ(_anti_join_node->output_column_references().at(1), _t_a_b);
  EXPECT_EQ(_anti_join_node->output_column_references().at(2), _t_a_c);
}

}  // namespace opossum
