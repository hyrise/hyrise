#include <memory>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "logical_query_plan/mock_node.cpp"
#include "logical_query_plan/union_node.hpp"

namespace opossum {

class UnionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    _union_node = UnionNode::make(UnionMode::Positions);
    _union_node->set_left_input(_mock_node);
    _union_node->set_right_input(_mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<UnionNode> _union_node;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
};

TEST_F(UnionNodeTest, Description) { EXPECT_EQ(_union_node->description(), "[UnionNode] Mode: UnionPositions"); }

TEST_F(UnionNodeTest, StatisticsNotImplemented) {
  EXPECT_THROW(_union_node->derive_statistics_from(_mock_node, _mock_node), std::exception);
}

TEST_F(UnionNodeTest, ColumnReferenceByNamedColumnReference) {
  EXPECT_EQ(_union_node->get_column({"a"}), _a);
  EXPECT_EQ(_union_node->get_column({"a", {"t_a"}}), _a);
  EXPECT_EQ(_union_node->get_column({"b"}), _b);
  EXPECT_EQ(_union_node->get_column({"b", {"t_a"}}), _b);
  EXPECT_EQ(_union_node->get_column({"c"}), _c);
  EXPECT_EQ(_union_node->get_column({"c", {"t_a"}}), _c);
}

TEST_F(UnionNodeTest, OutputColumnReferences) {
  EXPECT_EQ(_union_node->output_column_references().at(0), _a);
  EXPECT_EQ(_union_node->output_column_references().at(1), _b);
  EXPECT_EQ(_union_node->output_column_references().at(2), _c);
}

TEST_F(UnionNodeTest, MismatchingColumnNames) {
  /**
   * If the input tables have different column layouts get_verbose_column_name() will fail
   */
  auto mock_node_b = MockNode::make(
      MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "d"}, {DataType::Int, "c"}}, "t_a");

  auto invalid_union = UnionNode::make(UnionMode::Positions);
  invalid_union->set_left_input(_mock_node);
  invalid_union->set_right_input(mock_node_b);

  EXPECT_THROW(invalid_union->get_verbose_column_name(ColumnID{1}), std::exception);
}

TEST_F(UnionNodeTest, VerboseColumnNames) {
  /**
   * UnionNode will only prefix columns with its own ALIAS and forget any table names / aliases of its input tables
   */
  auto verbose_union = UnionNode::make(UnionMode::Positions);
  verbose_union->set_left_input(_mock_node);
  verbose_union->set_right_input(_mock_node);
  verbose_union->set_alias("union_alias");

  EXPECT_EQ(_union_node->get_verbose_column_name(ColumnID{0}), "a");
  EXPECT_EQ(_union_node->get_verbose_column_name(ColumnID{1}), "b");
  EXPECT_EQ(verbose_union->get_verbose_column_name(ColumnID{0}), "union_alias.a");
  EXPECT_EQ(verbose_union->get_verbose_column_name(ColumnID{1}), "union_alias.b");
}

TEST_F(UnionNodeTest, ShallowEquals) {
  EXPECT_TRUE(_union_node->shallow_equals(*_union_node));
  const auto other_union_node_a = UnionNode::make(UnionMode::Positions);
  EXPECT_TRUE(other_union_node_a->shallow_equals(*_union_node));
}

}  // namespace opossum
