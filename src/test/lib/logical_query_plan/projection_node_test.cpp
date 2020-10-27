#include <memory>

#include "base_test.hpp"

#include "expression/abstract_expression.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "storage/table_key_constraint.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ProjectionNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
    _c = _mock_node->get_column("c");

    // SELECT c, a, b, b+c, a+c
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b, add_(_b, _c), add_(_a, _c)), _mock_node);

    _key_constraint_a_b_pk = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
    _key_constraint_b = TableKeyConstraint{{ColumnID{1}}, KeyConstraintType::UNIQUE};
  }

  std::optional<TableKeyConstraint> _key_constraint_a_b_pk;
  std::optional<TableKeyConstraint> _key_constraint_b;
  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<ProjectionNode> _projection_node;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c;
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
  EXPECT_EQ(*_projection_node->node_expressions.at(0), *_c);
  EXPECT_EQ(*_projection_node->node_expressions.at(1), *_a);
  EXPECT_EQ(*_projection_node->node_expressions.at(2), *_b);
  EXPECT_EQ(*_projection_node->node_expressions.at(3), *add_(_b, _c));
  EXPECT_EQ(*_projection_node->node_expressions.at(4), *add_(_a, _c));
}

TEST_F(ProjectionNodeTest, UniqueConstraintsEmpty) {
  EXPECT_TRUE(_mock_node->unique_constraints()->empty());
  EXPECT_TRUE(_projection_node->unique_constraints()->empty());
}

TEST_F(ProjectionNodeTest, UniqueConstraintsReorderedColumns) {
  // Add constraints to MockNode
  _mock_node->set_key_constraints({*_key_constraint_a_b_pk, *_key_constraint_b});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 2);

  {
    // Reorder columns: (a, b, c) -> (c, a, b)
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b), _mock_node);

    // Basic check
    const auto& unique_constraints = _projection_node->unique_constraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_a_b_pk, unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_b, unique_constraints));
  }

  {
    // Reorder columns: (a, b, c) -> (b, c, a)
    _projection_node = ProjectionNode::make(expression_vector(_c, _a, _b), _mock_node);

    // Basic check
    const auto& unique_constraints = _projection_node->unique_constraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_a_b_pk, unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_b, unique_constraints));
  }
}

TEST_F(ProjectionNodeTest, UniqueConstraintsRemovedColumns) {
  // Prepare two unique constraints for MockNode
  _mock_node->set_key_constraints({*_key_constraint_a_b_pk, *_key_constraint_b});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 2);

  // Test (a, b, c) -> (a, c) - no more constraints valid
  _projection_node = ProjectionNode::make(expression_vector(_a, _c), _mock_node);
  EXPECT_TRUE(_projection_node->unique_constraints()->empty());

  // Test (a, b, c) -> (c) - no more constraints valid
  _projection_node = ProjectionNode::make(expression_vector(_c), _mock_node);
  EXPECT_TRUE(_projection_node->unique_constraints()->empty());

  {
    // Test (a, b, c) -> (a, b) - all constraints remain valid
    _projection_node = ProjectionNode::make(expression_vector(_a, _b), _mock_node);

    // Basic check
    const auto& unique_constraints = _projection_node->unique_constraints();
    EXPECT_EQ(unique_constraints->size(), 2);
    // In-depth check
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_a_b_pk, unique_constraints));
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_b, unique_constraints));
  }

  {
    // Test (a, b, c) -> (b) - unique constraint for b remains valid
    _projection_node = ProjectionNode::make(expression_vector(_b), _mock_node);

    // Basic check
    const auto& unique_constraints = _projection_node->unique_constraints();
    EXPECT_EQ(unique_constraints->size(), 1);
    // In-depth check
    EXPECT_TRUE(find_unique_constraint_by_key_constraint(*_key_constraint_b, unique_constraints));
  }
}

TEST_F(ProjectionNodeTest, FunctionalDependenciesForwarding) {
  // Preparations
  const auto fd_a = FunctionalDependency{{_a}, {_c}};
  const auto fd_b = FunctionalDependency{{_b}, {_c}};
  const auto fd_b_two_dependents = FunctionalDependency{{_b}, {_a, _c}};
  _mock_node->set_non_trivial_functional_dependencies({fd_a, fd_b, fd_b_two_dependents});
  EXPECT_EQ(_mock_node->functional_dependencies().size(), 3);

  // Tests
  // FDs without dependents are discarded
  const auto& projection_node_1 = ProjectionNode::make(expression_vector(_a, add_(_b, _c)), _mock_node);
  EXPECT_TRUE(projection_node_1->functional_dependencies().empty());
  const auto& projection_node_2 = ProjectionNode::make(expression_vector(_b, sub_(_b, _c)), _mock_node);
  EXPECT_TRUE(projection_node_2->functional_dependencies().empty());

  // Missing determinants lead to FD removal
  const auto& projection_node_3 = ProjectionNode::make(expression_vector(_a, _c), _mock_node);
  EXPECT_EQ(projection_node_3->functional_dependencies().size(), 1);
  EXPECT_EQ(projection_node_3->functional_dependencies().at(0), fd_a);

  // FDs are adjusted if some, but not all dependents are missing
  const auto& projection_node_4 = ProjectionNode::make(expression_vector(_a, _b), _mock_node);
  EXPECT_EQ(projection_node_4->functional_dependencies().size(), 1);
  const auto expected_fd = FunctionalDependency{{_b}, {_a}};
  EXPECT_EQ(projection_node_4->functional_dependencies().at(0), expected_fd);
}

}  // namespace opossum
