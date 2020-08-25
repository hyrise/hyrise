#include <memory>
#include <string>
#include <vector>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "types.hpp"
#include "utils/constraint_test_utils.hpp"

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
    _group_by_expressions = expression_vector(_a, _c);
    _aggregate_expressions = expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c)));
    _aggregate_node = AggregateNode::make(_group_by_expressions, _aggregate_expressions, _mock_node);
  }

  std::shared_ptr<MockNode> _mock_node;
  std::shared_ptr<AggregateNode> _aggregate_node;
  std::vector<std::shared_ptr<AbstractExpression>> _group_by_expressions, _aggregate_expressions;
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

TEST_F(AggregateNodeTest, UniqueConstraintsAdd) {
  EXPECT_TRUE(_mock_node->unique_constraints()->empty());

  const auto aggregate1 = sum_(add_(_a, _b));
  const auto aggregate2 = sum_(add_(_a, _c));
  const auto agg_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);
  const auto agg_node_b =
      AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate1, aggregate2), _mock_node);

  // Check whether AggregateNode adds a new unique constraint for its group-by column(s)
  {
    EXPECT_EQ(agg_node_a->unique_constraints()->size(), 1);
    const auto unique_constraint = *agg_node_a->unique_constraints()->cbegin();
    EXPECT_EQ(unique_constraint.expressions.size(), 1);
    EXPECT_TRUE(unique_constraint.expressions.contains(_a));
  }
  {
    EXPECT_EQ(agg_node_b->unique_constraints()->size(), 1);
    const auto unique_constraint = *agg_node_b->unique_constraints()->cbegin();
    EXPECT_EQ(unique_constraint.expressions.size(), 2);
    EXPECT_TRUE(unique_constraint.expressions.contains(_a));
    EXPECT_TRUE(unique_constraint.expressions.contains(_b));
  }
}

TEST_F(AggregateNodeTest, UniqueConstraintsForwardingSimple) {
  const auto key_constraint_b = TableKeyConstraint{{_b->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 2);

  const auto aggregate_c = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate_c), _mock_node);
  const auto& unique_constraints = _aggregate_node->unique_constraints();

  /**
   * Expected behaviour:
   *  - key_constraint_b remains valid since _b is part of the group-by columns.
   *  - key_constraint_c, however, should be discarded because _c gets aggregated.
   */

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_b, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsForwardingAnyAggregates) {
  const auto key_constraint_b = TableKeyConstraint{{_b->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 2);

  const auto aggregate_b = any_(_b);
  const auto aggregate_c = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a), expression_vector(aggregate_b, aggregate_c), _mock_node);
  const auto& unique_constraints = _aggregate_node->unique_constraints();

  /**
   * Expected behaviour:
   *  - key_constraint_b remains valid because _b is aggregated via ANY(), a pseudo aggregate function used
   *    by the DependentGroupByReductionRule to optimize group-bys.
   *  - key_constraint_c should be discarded because _c is aggregated.
   *  - Also, we should gain a new unique constraint, covering all group-by columns.
   */

  // Basic check
  EXPECT_EQ(unique_constraints->size(), 2);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_b, unique_constraints));
  const auto key_constraint_group_by = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(key_constraint_group_by, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsNoDuplicates) {
  // Prepare single unique constraint
  const auto table_key_constraint = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_key_constraint});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 1);

  const auto aggregate1 = sum_(_b);
  const auto aggregate2 = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);

  /**
   * AggregateNode should try to create a new unique constraint from its group-by-column _a. It is the same as
   * MockNode's unique constraint which gets forwarded.
   *
   * Expected behaviour: AggregateNode should not output the same unique constraint twice.
   */

  // Basic check
  const auto& unique_constraints = _aggregate_node->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(table_key_constraint, unique_constraints));
}

TEST_F(AggregateNodeTest, UniqueConstraintsNoSupersets) {
  // Prepare single unique constraint
  const auto table_key_constraint = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_key_constraint});
  EXPECT_EQ(_mock_node->unique_constraints()->size(), 1);

  const auto aggregate = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate), _mock_node);

  /**
   * AggregateNode should try to create a new unique constraint from both group-by-columns _a and _b.
   * However, MockNode already has a unique constraint for _a which gets forwarded. It is shorter and
   * therefore preferred over the unique constraint covering both, _a and _b.
   *
   * Expected behaviour: AggregateNode should forward the input unique constraint only.
   */

  // Basic check
  const auto& unique_constraints = _aggregate_node->unique_constraints();
  EXPECT_EQ(unique_constraints->size(), 1);
  // In-depth check
  EXPECT_TRUE(find_unique_constraint_by_key_constraint(table_key_constraint, unique_constraints));
}

TEST_F(AggregateNodeTest, FunctionalDependenciesForwarding) {
  // Preparations
  const auto fd_a = FunctionalDependency{{_a}, {_c}};
  const auto fd_b_two_dependents = FunctionalDependency{{_b}, {_a, _c}};
  _mock_node->set_non_trivial_functional_dependencies({fd_a, fd_b_two_dependents});
  EXPECT_EQ(_mock_node->functional_dependencies().size(), 2);

  const auto aggregate1 = sum_(add_(_a, _b));
  const auto aggregate2 = sum_(add_(_a, _c));

  // All determinant and dependent expressions are missing.
  const auto& agg_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);
  EXPECT_TRUE(agg_node_a->non_trivial_functional_dependencies().empty());

  // All determinant and dependent expressions are part of the output -> expect FD forwarding
  const auto& agg_node_b =
      AggregateNode::make(expression_vector(_a, _c), expression_vector(aggregate1, aggregate2), _mock_node);
  EXPECT_EQ(agg_node_b->non_trivial_functional_dependencies().size(), 1);
  EXPECT_EQ(agg_node_b->non_trivial_functional_dependencies().at(0), fd_a);

  // Special case: All determinant expressions, but only some of the dependent expressions are part of the output
  const auto& agg_node_c =
      AggregateNode::make(expression_vector(_b, _c), expression_vector(aggregate1, aggregate2), _mock_node);
  const auto expected_fd = FunctionalDependency{{_b}, {_c}};
  EXPECT_EQ(agg_node_c->non_trivial_functional_dependencies().size(), 1);
  EXPECT_EQ(agg_node_c->non_trivial_functional_dependencies().at(0), expected_fd);
}

TEST_F(AggregateNodeTest, FunctionalDependenciesAdd) {
  // The group-by columns form a new candidate key / unique constraint from which we should derive a trivial FD.
  _mock_node->set_key_constraints({});
  _mock_node->set_non_trivial_functional_dependencies({});

  const auto& fds = _aggregate_node->functional_dependencies();
  EXPECT_EQ(fds.size(), 1);
  const auto& fd = fds.at(0);
  const auto expected_determinants =
      ExpressionUnorderedSet{_group_by_expressions.cbegin(), _group_by_expressions.cend()};
  const auto expected_dependents =
      ExpressionUnorderedSet{_aggregate_expressions.cbegin(), _aggregate_expressions.cend()};
  EXPECT_EQ(fd.determinants, expected_determinants);
  EXPECT_EQ(fd.dependents, expected_dependents);
}

}  // namespace opossum
