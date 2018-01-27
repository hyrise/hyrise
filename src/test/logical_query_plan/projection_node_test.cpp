#include <algorithm>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "abstract_expression.hpp"
#include "logical_query_plan/lqp_expression.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "storage/storage_manager.hpp"

namespace opossum {

class ProjectionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = std::make_shared<MockNode>(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    const auto a_expr = LQPExpression::create_column(_a);
    const auto b_expr = LQPExpression::create_column(_b);
    const auto c_expr = LQPExpression::create_column(_c);

    // SELECT c, a, b AS alias_for_b, b+c AS some_addition, a+c [...]
    _projection_node = std::make_shared<ProjectionNode>(std::vector<std::shared_ptr<LQPExpression>>{
        c_expr, a_expr, LQPExpression::create_column(_b, {"alias_for_b"}),
        LQPExpression::create_binary_operator(ExpressionType::Addition, b_expr, c_expr, {"some_addition"}),
        LQPExpression::create_binary_operator(ExpressionType::Addition, a_expr, c_expr)});
    _projection_node->set_left_child(_mock_node);

    _some_addition = {_projection_node, ColumnID{3}};
    _a_plus_c = {_projection_node, ColumnID{4}};
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<ProjectionNode> _projection_node;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
  LQPColumnReference _some_addition;
  LQPColumnReference _a_plus_c;
};

TEST_F(ProjectionNodeTest, Description) {
  EXPECT_EQ(_projection_node->description(), "[Projection] t_a.c, t_a.a, t_a.b, t_a.b + t_a.c, t_a.a + t_a.c");
}

TEST_F(ProjectionNodeTest, ColumnReferenceByNamedColumnReference) {
  EXPECT_EQ(_projection_node->get_column({"c", std::nullopt}), _c);
  EXPECT_EQ(_projection_node->get_column({"c", "t_a"}), _c);
  EXPECT_EQ(_projection_node->get_column({"a", std::nullopt}), _a);
  EXPECT_EQ(_projection_node->get_column({"a", "t_a"}), _a);
  EXPECT_EQ(_projection_node->get_column({"alias_for_b", std::nullopt}), _b);
  EXPECT_EQ(_projection_node->find_column({"alias_for_b", "t_a"}), std::nullopt);
  EXPECT_EQ(_projection_node->get_column({"some_addition", std::nullopt}), _some_addition);
  EXPECT_EQ(_projection_node->find_column({"some_addition", "t_a"}), std::nullopt);
}

TEST_F(ProjectionNodeTest, ColumnReferenceByOutputColumnID) {
  ASSERT_EQ(_projection_node->output_column_references().size(), 5u);
  EXPECT_EQ(_projection_node->output_column_references().at(0), _c);
  EXPECT_EQ(_projection_node->output_column_references().at(1), _a);
  EXPECT_EQ(_projection_node->output_column_references().at(2), _b);
  EXPECT_EQ(_projection_node->output_column_references().at(3), _some_addition);
  EXPECT_EQ(_projection_node->output_column_references().at(4), _a_plus_c);
}

TEST_F(ProjectionNodeTest, VerboseColumnNames) {
  EXPECT_EQ(_projection_node->get_verbose_column_name(ColumnID{0}), "t_a.c");
  EXPECT_EQ(_projection_node->get_verbose_column_name(ColumnID{1}), "t_a.a");
  EXPECT_EQ(_projection_node->get_verbose_column_name(ColumnID{2}), "alias_for_b");
  EXPECT_EQ(_projection_node->get_verbose_column_name(ColumnID{3}), "some_addition");
}

}  // namespace opossum
