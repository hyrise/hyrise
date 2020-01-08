#include <algorithm>
#include <memory>
#include <vector>

#include "gtest/gtest.h"

#include "base_test.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ProjectionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _table_constraints = {};
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
    _c = _mock_node->get_column("c");

    // SELECT c, a, b, b+c AS some_addition, a+c [...]
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b, add_(_b, _c), add_(_a, _c)), _mock_node);
  }

  void recreate_mock_node_with_constraints(TableConstraintDefinitions table_constraints_in = {}) {

    if(table_constraints_in.empty()) {
      // Primary Key: a, b
      const auto table_constraint_1 =
          TableConstraintDefinition{std::vector<ColumnID>{ColumnID{0}, ColumnID{1}}, IsPrimaryKey::Yes};
      // Unique: b
      const auto table_constraint_2 = TableConstraintDefinition{std::vector<ColumnID>{ColumnID{1}}, IsPrimaryKey::No};

      _table_constraints = TableConstraintDefinitions{table_constraint_1, table_constraint_2};
    } else {
      _table_constraints = table_constraints_in;
    }

    _mock_node =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}},
                       "t_a", _table_constraints);

    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
    _c = _mock_node->get_column("c");

    _projection_node = nullptr;
  }

  TableConstraintDefinitions _table_constraints;
  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<ProjectionNode> _projection_node;
  LQPColumnReference _a, _b, _c;
};

TEST_F(ProjectionNodeTest, Description) {
  EXPECT_EQ(_projection_node->description(), "[Projection] c, a, b, b + c, a + c");
}

TEST_F(ProjectionNodeTest, HashingAndEqualityCheck) {
  EXPECT_EQ(*_projection_node, *_projection_node);

  const auto different_projection_node_a =
      ProjectionNode::make(expression_vector(_a, _c, _b, add_(_b, _c), add_(_a, _c)), _mock_node);
  const auto different_projection_node_b =
      ProjectionNode::make(expression_vector(_c, _a, _b, add_(_b, _c)), _mock_node);
  EXPECT_NE(*_projection_node, *different_projection_node_a);
  EXPECT_NE(*_projection_node, *different_projection_node_b);

  EXPECT_NE(_projection_node->hash(), different_projection_node_a->hash());
  EXPECT_NE(_projection_node->hash(), different_projection_node_b->hash());
}

TEST_F(ProjectionNodeTest, Copy) { EXPECT_EQ(*_projection_node->deep_copy(), *_projection_node); }

TEST_F(ProjectionNodeTest, NodeExpressions) {
  ASSERT_EQ(_projection_node->node_expressions.size(), 5u);
  EXPECT_EQ(*_projection_node->node_expressions.at(0), *lqp_column_(_c));
  EXPECT_EQ(*_projection_node->node_expressions.at(1), *lqp_column_(_a));
  EXPECT_EQ(*_projection_node->node_expressions.at(2), *lqp_column_(_b));
  EXPECT_EQ(*_projection_node->node_expressions.at(3), *add_(_b, _c));
  EXPECT_EQ(*_projection_node->node_expressions.at(4), *add_(_a, _c));
}

TEST_F(ProjectionNodeTest, ConstraintsEmpty) {
  EXPECT_TRUE(_mock_node->constraints()->empty());
  EXPECT_TRUE(_projection_node->constraints()->empty());
}

TEST_F(ProjectionNodeTest, ConstraintsReorderedColumns) {

  // Recreate MockNode to incorporate two constraints
  recreate_mock_node_with_constraints();
  EXPECT_EQ(_mock_node->constraints()->size(), 2);

  { // Reorder columns: (a, b, c) -> (c, a, b)
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b), _mock_node);

    // Basic check
    const auto lqp_constraints = _projection_node->constraints();
    EXPECT_EQ(lqp_constraints->size(), 2);
    // In-depth check
    check_table_constraint_representation(_table_constraints, lqp_constraints);
  }

  { // Reorder columns: (a, b, c) -> (b, c, a)
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b), _mock_node);

    // Basic check
    const auto lqp_constraints = _projection_node->constraints();
    EXPECT_EQ(lqp_constraints->size(), 2);
    // In-depth check
    check_table_constraint_representation(_table_constraints, lqp_constraints);
  }
}

TEST_F(ProjectionNodeTest, ConstraintsRemovedColumns) {

  // Recreate MockNode to incorporate two constraints
  recreate_mock_node_with_constraints();
  EXPECT_EQ(_mock_node->constraints()->size(), 2);

  { // Test (a, b, c) -> (a, c) - no more constraints valid
    _projection_node = ProjectionNode::make(expression_vector(_a, _c), _mock_node);
    EXPECT_TRUE(_projection_node->constraints()->empty());
  }

  { // Test (a, b, c) -> (c) - no more constraints valid
    _projection_node = ProjectionNode::make(expression_vector(_c), _mock_node);
    EXPECT_TRUE(_projection_node->constraints()->empty());
  }

  { // Test (a, b, c) -> (a, b) - all constraints remain valid
    _projection_node = ProjectionNode::make(expression_vector(_a, _b), _mock_node);

    // Basic check
    const auto lqp_constraints = _projection_node->constraints();
    EXPECT_EQ(lqp_constraints->size(), 2);
    // In-depth check
    check_table_constraint_representation(_table_constraints, lqp_constraints);
  }

  { // Test (a, b, c) -> (b) - unique constraint for b remains valid
    _projection_node = ProjectionNode::make(expression_vector(_b), _mock_node);

    // Basic check
    const auto lqp_constraints = _projection_node->constraints();
    EXPECT_EQ(lqp_constraints->size(), 1);
    // In-depth check
    check_table_constraint_representation({_table_constraints.at(1)}, lqp_constraints);
  }

}

}  // namespace opossum
