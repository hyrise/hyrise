#include <memory>
#include <string>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "utils/constraint_test_utils.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class MockNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});
    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a;
  std::shared_ptr<MockNode> _mock_node_b;
};

TEST_F(MockNodeTest, Description) {
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] Columns: a b c d | pruned: 0/4 columns");
  EXPECT_EQ(_mock_node_b->description(), "[MockNode 'mock_name'] Columns: a b | pruned: 0/2 columns");

  _mock_node_a->set_pruned_column_ids({ColumnID{2}});
  EXPECT_EQ(_mock_node_a->description(), "[MockNode 'Unnamed'] Columns: a b d | pruned: 1/4 columns");
}

TEST_F(MockNodeTest, OutputColumnExpression) {
  ASSERT_EQ(_mock_node_a->output_expressions().size(), 4u);
  EXPECT_EQ(*_mock_node_a->output_expressions().at(0), *lqp_column_(_mock_node_a, ColumnID{0}));
  EXPECT_EQ(*_mock_node_a->output_expressions().at(1), *lqp_column_(_mock_node_a, ColumnID{1}));
  EXPECT_EQ(*_mock_node_a->output_expressions().at(2), *lqp_column_(_mock_node_a, ColumnID{2}));
  EXPECT_EQ(*_mock_node_a->output_expressions().at(3), *lqp_column_(_mock_node_a, ColumnID{3}));

  ASSERT_EQ(_mock_node_b->output_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_b->output_expressions().at(0), *lqp_column_(_mock_node_b, ColumnID{0}));
  EXPECT_EQ(*_mock_node_b->output_expressions().at(1), *lqp_column_(_mock_node_b, ColumnID{1}));

  _mock_node_a->set_pruned_column_ids({ColumnID{0}, ColumnID{3}});
  EXPECT_EQ(_mock_node_a->output_expressions().size(), 2u);
  EXPECT_EQ(*_mock_node_a->output_expressions().at(0), *lqp_column_(_mock_node_a, ColumnID{1}));
  EXPECT_EQ(*_mock_node_a->output_expressions().at(1), *lqp_column_(_mock_node_a, ColumnID{2}));
}

TEST_F(MockNodeTest, HashingAndEqualityCheck) {
  const auto same_mock_node_b =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  const auto different_mock_node_1 =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Long, "a"}, {DataType::String, "b"}}, "mock_name");
  const auto different_mock_node_2 =
      MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "other_name");
  EXPECT_EQ(*_mock_node_b, *_mock_node_b);
  EXPECT_NE(*_mock_node_b, *different_mock_node_1);
  EXPECT_NE(*_mock_node_b, *different_mock_node_2);
  EXPECT_EQ(*_mock_node_b, *same_mock_node_b);

  EXPECT_NE(_mock_node_b->hash(), different_mock_node_1->hash());
  EXPECT_EQ(_mock_node_b->hash(), different_mock_node_2->hash());
  EXPECT_EQ(_mock_node_b->hash(), same_mock_node_b->hash());
}

TEST_F(MockNodeTest, Copy) {
  const auto copy = _mock_node_b->deep_copy();
  EXPECT_EQ(*_mock_node_b, *copy);

  _mock_node_b->set_pruned_column_ids({ColumnID{1}});
  EXPECT_NE(*_mock_node_b, *copy);
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());
}

TEST_F(MockNodeTest, NodeExpressions) { ASSERT_EQ(_mock_node_a->node_expressions.size(), 0u); }

TEST_F(MockNodeTest, UniqueConstraints) {
  // Add constraints to MockNode
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto table_key_constraints = TableKeyConstraints{key_constraint_a_b, key_constraint_c};
  _mock_node_a->set_key_constraints(table_key_constraints);

  // Basic checks
  const auto& unique_constraints_mock_node_a = _mock_node_a->unique_constraints();
  EXPECT_EQ(unique_constraints_mock_node_a->size(), 2);
  EXPECT_TRUE(_mock_node_b->unique_constraints()->empty());

  // In-depth verification
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_a_b, unique_constraints_mock_node_a));
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints_mock_node_a));

  // Check whether MockNode is referenced by the constraint's expressions
  for (const auto& unique_constraint : *unique_constraints_mock_node_a) {
    for (const auto& expression : unique_constraint.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node.expired());
      EXPECT_TRUE(column_expression->original_node.lock() == _mock_node_a);
    }
  }
}

TEST_F(MockNodeTest, UniqueConstraintsPrunedColumns) {
  // Prepare unique constraints
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node_a->set_key_constraints({key_constraint_a, key_constraint_a_b, key_constraint_c});
  EXPECT_EQ(_mock_node_a->key_constraints().size(), 3);
  auto unique_constraints = _mock_node_a->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 3);

  // Prune column a, which should remove two unique constraints
  _mock_node_a->set_pruned_column_ids({ColumnID{0}});

  // Basic check
  unique_constraints = _mock_node_a->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_c, unique_constraints));
}

}  // namespace opossum
