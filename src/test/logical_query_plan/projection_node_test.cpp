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
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = {_mock_node, ColumnID{0}};
    _b = {_mock_node, ColumnID{1}};
    _c = {_mock_node, ColumnID{2}};

    _a_expr = LQPExpression::create_column(_a);
    _b_expr = LQPExpression::create_column(_b);
    _c_expr = LQPExpression::create_column(_c);

    // SELECT c, a, b AS alias_for_b, b+c AS some_addition, a+c [...]
    _projection_node = ProjectionNode::make(std::vector<std::shared_ptr<LQPExpression>>{
        _c_expr, _a_expr, LQPExpression::create_column(_b, {"alias_for_b"}),
        LQPExpression::create_binary_operator(ExpressionType::Addition, _b_expr, _c_expr, {"some_addition"}),
        LQPExpression::create_binary_operator(ExpressionType::Addition, _a_expr, _c_expr)});
    _projection_node->set_left_input(_mock_node);

    _some_addition = {_projection_node, ColumnID{3}};
    _a_plus_c = {_projection_node, ColumnID{4}};
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<ProjectionNode> _projection_node;
  std::shared_ptr<LQPExpression> _a_expr, _b_expr, _c_expr;
  LQPColumnReference _a;
  LQPColumnReference _b;
  LQPColumnReference _c;
  LQPColumnReference _some_addition;
  LQPColumnReference _a_plus_c;
};

TEST_F(ProjectionNodeTest, Description) {
  EXPECT_EQ(_projection_node->description(),
            "[Projection] t_a.c, t_a.a, t_a.b AS alias_for_b, t_a.b + t_a.c, t_a.a + t_a.c");
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

TEST_F(ProjectionNodeTest, ShallowEquals) {
  EXPECT_TRUE(_projection_node->shallow_equals(*_projection_node));

  const auto other_projection_node_a = ProjectionNode::make(
      std::vector<std::shared_ptr<LQPExpression>>{
          _c_expr, _a_expr, LQPExpression::create_column(_b, {"alias_for_b"}),
          LQPExpression::create_binary_operator(ExpressionType::Addition, _b_expr, _c_expr, {"some_addition"}),
          LQPExpression::create_binary_operator(ExpressionType::Addition, _a_expr, _c_expr)},
      _mock_node);
  EXPECT_TRUE(other_projection_node_a->shallow_equals(*_projection_node));

  const auto other_projection_node_b = ProjectionNode::make(
      std::vector<std::shared_ptr<LQPExpression>>{
          _c_expr, _a_expr, LQPExpression::create_column(_b, {"alias_for_bs"}),
          LQPExpression::create_binary_operator(ExpressionType::Addition, _b_expr, _c_expr, {"some_addition"}),
          LQPExpression::create_binary_operator(ExpressionType::Addition, _a_expr, _c_expr)},
      _mock_node);
  EXPECT_FALSE(other_projection_node_b->shallow_equals(*_projection_node));
}

}  // namespace opossum
