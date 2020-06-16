#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class AggregateNodeTest : public BaseTest {
 protected:
  void SetUp() override {
    _mock_node = MockNode::make(
        MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Int, "b"}, {DataType::Int, "c"}}, "t_a");

    _a = _mock_node->get_column("a");
    _b = _mock_node->get_column("b");
    _c = _mock_node->get_column("c");

    // SELECT a, c, SUM(a+b), SUM(a+c) AS some_sum [...] GROUP BY a, c
    // Columns are ordered as specified in the SELECT list
    _aggregate_node = AggregateNode::make(expression_vector(_a, _c),
                                          expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  std::shared_ptr<LQPColumnExpression> _a, _b, _c;
};

TEST_F(AggregateNodeTest, OutputColumnExpressions) {
  ASSERT_EQ(_aggregate_node->output_expressions().size(), 4u);
  EXPECT_EQ(*_aggregate_node->output_expressions().at(0), *_a);
  EXPECT_EQ(*_aggregate_node->output_expressions().at(1), *_c);
  EXPECT_EQ(*_aggregate_node->output_expressions().at(2), *sum_(add_(_a, _b)));
  EXPECT_EQ(*_aggregate_node->output_expressions().at(3), *sum_(add_(_a, _c)));
}

TEST_F(AggregateNodeTest, NodeExpressions) {
  ASSERT_EQ(_aggregate_node->node_expressions.size(), 4u);
  EXPECT_EQ(*_aggregate_node->node_expressions.at(0), *_a);
  EXPECT_EQ(*_aggregate_node->node_expressions.at(1), *_c);
  EXPECT_EQ(*_aggregate_node->node_expressions.at(2), *sum_(add_(_a, _b)));
  EXPECT_EQ(*_aggregate_node->node_expressions.at(3), *sum_(add_(_a, _c)));
}

TEST_F(AggregateNodeTest, Description) {
  auto description = _aggregate_node->description();

  EXPECT_EQ(description, "[Aggregate] GroupBy: [a, c] Aggregates: [SUM(a + b), SUM(a + c)]");
}

TEST_F(AggregateNodeTest, HashingAndEqualityCheck) {
  const auto same_aggregate_node = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);

  EXPECT_EQ(*_aggregate_node, *same_aggregate_node);
  EXPECT_EQ(*same_aggregate_node, *_aggregate_node);
  EXPECT_EQ(*_aggregate_node, *_aggregate_node);

  // Build slightly different aggregate nodes
  const auto different_aggregate_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  const auto different_aggregate_node_b = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, 2)), sum_(add_(_a, _c))), _mock_node);
  const auto different_aggregate_node_c = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c)), min_(_a)), _mock_node);
  const auto different_aggregate_node_d = AggregateNode::make(
      expression_vector(_a, _a), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);

  EXPECT_NE(*_aggregate_node, *different_aggregate_node_a);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_b);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_c);
  EXPECT_NE(*_aggregate_node, *different_aggregate_node_d);

  EXPECT_NE(_aggregate_node->hash(), different_aggregate_node_a->hash());
  // _aggregate_node and different_aggregate_node_b are known to conflict because we do not recurse deep enough to
  // identify the difference in the aggregate expressions. That is acceptable, as long as the comparison identifies
  // the two nodes as non-equal.
  EXPECT_NE(_aggregate_node->hash(), different_aggregate_node_c->hash());
  EXPECT_NE(_aggregate_node->hash(), different_aggregate_node_d->hash());
}

TEST_F(AggregateNodeTest, Copy) {
  const auto same_aggregate_node = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  EXPECT_EQ(*_aggregate_node->deep_copy(), *same_aggregate_node);
}

TEST_F(AggregateNodeTest, ConstraintsAddNewConstraint) {
  EXPECT_TRUE(_mock_node->unique_constraints()->empty());

  const auto aggregate1 = sum_(add_(_a, _b));
  const auto aggregate2 = sum_(add_(_a, _c));
  const auto agg_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);
  const auto agg_node_b =
      AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate1, aggregate2), _mock_node);

  // Check, whether AggregateNode adds a new constraint for its group-by column(s)
  EXPECT_EQ(agg_node_a->unique_constraints()->size(), 1);
  EXPECT_EQ(agg_node_b->unique_constraints()->size(), 1);
  const auto lqp_constraint_a = *agg_node_a->unique_constraints()->cbegin();
  const auto lqp_constraint_b = *agg_node_b->unique_constraints()->cbegin();
  EXPECT_EQ(lqp_constraint_a.column_expressions.size(), 1);
  EXPECT_EQ(lqp_constraint_b.column_expressions.size(), 2);
  EXPECT_TRUE((lqp_constraint_a.column_expressions.contains(_a)));
  EXPECT_TRUE((lqp_constraint_b.column_expressions.contains(_a)));
  EXPECT_TRUE((lqp_constraint_b.column_expressions.contains(_b)));
}

TEST_F(AggregateNodeTest, ConstraintsForwarding) {
  // Prepare Test
  const auto table_constraint_1 = TableKeyConstraint{{_b->original_column_id}, KeyConstraintType::UNIQUE};
  const auto table_constraint_2 = TableKeyConstraint{{_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_constraint_1, table_constraint_2});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 2);

  const auto aggregate = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate), _mock_node);

  // Since _b is part of the group-by columns, table_constraint1 remains valid.
  // As _c becomes aggregated, table_constraint2 has to be discarded
  // In addition, a new table constraint is created covering all group-by columns (_a, _b)
  EXPECT_EQ(_aggregate_node->unique_constraints()->size(), 2);
}

TEST_F(AggregateNodeTest, ConstraintsNoDuplicates) {
  // Prepare Test
  const auto table_constraint = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_constraint});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 1);

  const auto aggregate = sum_(_b);
  _aggregate_node = AggregateNode::make(expression_vector(_a), expression_vector(aggregate), _mock_node);

  // The AggregateNode should create a new unique constraint based on column _a
  // However, table_constraint is equivalent and eligible for forwarding.
  // We do not want AggregateNode to output two constraints that are equivalent.
  EXPECT_EQ(_aggregate_node->unique_constraints()->size(), 1);
  const auto constraint = *_aggregate_node->unique_constraints()->cbegin();
  EXPECT_TRUE(constraint.column_expressions.contains(_a));
}

}  // namespace opossum
