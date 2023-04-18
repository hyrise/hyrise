#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "statistics/generate_pruning_statistics.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

class MockNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node_a = MockNode::make(MockNode::ColumnDefinitions{
        {DataType::Int, "a"}, {DataType::Float, "b"}, {DataType::Double, "c"}, {DataType::String, "d"}});
    _mock_node_b =
        MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}}, "mock_name");
  }

  std::shared_ptr<MockNode> _mock_node_a, _mock_node_b;
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
  std::string comment = "Special MockNode.";
  _mock_node_b->comment = comment;

  const auto copy = _mock_node_b->deep_copy();
  EXPECT_EQ(*_mock_node_b, *copy);
  EXPECT_EQ(copy->comment, comment);

  _mock_node_b->set_pruned_column_ids({ColumnID{1}});
  EXPECT_NE(*_mock_node_b, *copy);
  EXPECT_EQ(*_mock_node_b, *_mock_node_b->deep_copy());
}

TEST_F(MockNodeTest, NodeExpressions) {
  ASSERT_EQ(_mock_node_a->node_expressions.size(), 0u);
}

TEST_F(MockNodeTest, UniqueColumnCombinations) {
  // Add constraints to MockNode.
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::PRIMARY_KEY};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  const auto table_key_constraints = TableKeyConstraints{key_constraint_a_b, key_constraint_c};
  _mock_node_a->set_key_constraints(table_key_constraints);

  EXPECT_TRUE(_mock_node_b->unique_column_combinations().empty());

  const auto& unique_column_combinations = _mock_node_a->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 2);
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a_b, unique_column_combinations));
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));

  // Check whether MockNode is referenced by the UCC's expressions.
  for (const auto& ucc : unique_column_combinations) {
    for (const auto& expression : ucc.expressions) {
      const auto& column_expression = std::dynamic_pointer_cast<LQPColumnExpression>(expression);
      EXPECT_TRUE(column_expression && !column_expression->original_node.expired());
      EXPECT_TRUE(column_expression->original_node.lock() == _mock_node_a);
    }
  }
}

TEST_F(MockNodeTest, UniqueColumnCombinationsPrunedColumns) {
  // Prepare UCCs.
  const auto key_constraint_a = TableKeyConstraint{{ColumnID{0}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_a_b = TableKeyConstraint{{ColumnID{0}, ColumnID{1}}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{ColumnID{2}}, KeyConstraintType::UNIQUE};
  _mock_node_a->set_key_constraints({key_constraint_a, key_constraint_a_b, key_constraint_c});
  EXPECT_EQ(_mock_node_a->key_constraints().size(), 3);

  {
    const auto& unique_column_combinations = _mock_node_a->unique_column_combinations();
    EXPECT_EQ(unique_column_combinations.size(), 3);
    EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a, unique_column_combinations));
    EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_a_b, unique_column_combinations));
    EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));
  }
  {
    // Prune column a, which should remove two UCCs.
    _mock_node_a->set_pruned_column_ids({ColumnID{0}});

    const auto& unique_column_combinations = _mock_node_a->unique_column_combinations();
    EXPECT_EQ(unique_column_combinations.size(), 1);
    EXPECT_FALSE(find_ucc_by_key_constraint(key_constraint_a, unique_column_combinations));
    EXPECT_FALSE(find_ucc_by_key_constraint(key_constraint_a_b, unique_column_combinations));
    EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));
  }
}

TEST_F(MockNodeTest, OrderDependencies) {
  const auto od_a_to_b = OrderDependency{{_mock_node_a->get_column("a")}, {_mock_node_a->get_column("b")}};
  const auto od_a_to_c = OrderDependency{{_mock_node_a->get_column("a")}, {_mock_node_a->get_column("c")}};
  const auto order_constraint_a_to_b = TableOrderConstraint{{ColumnID{0}}, {ColumnID{1}}};
  const auto order_constraint_a_to_c = TableOrderConstraint{{ColumnID{0}}, {ColumnID{2}}};
  _mock_node_a->set_order_constraints({order_constraint_a_to_b, order_constraint_a_to_c});

  // Forward ODs.
  {
    const auto& order_dependencies = _mock_node_a->order_dependencies();
    EXPECT_EQ(order_dependencies.size(), 2);
    EXPECT_TRUE(order_dependencies.contains(od_a_to_b));
    EXPECT_TRUE(order_dependencies.contains(od_a_to_c));
  }

  // Discard ODs that involve pruned columns.
  {
    _mock_node_a->set_pruned_column_ids({ColumnID{0}, ColumnID{2}});
    const auto& order_dependencies = _mock_node_a->order_dependencies();
    EXPECT_TRUE(order_dependencies.empty());
  }
}

}  // namespace hyrise
