#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "expression/expression_utils.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/mock_node.hpp"
#include "types.hpp"
#include "utils/data_dependency_test_utils.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

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

TEST_F(AggregateNodeTest, InvalidAggregates) {
  if constexpr (!HYRISE_DEBUG) {
    GTEST_SKIP();
  }

  // Aggregate expression must be an aggregate function (i.e., a WindowFunctionExpression).
  EXPECT_THROW(AggregateNode::make(expression_vector(), expression_vector(add_(_a, _b))), std::logic_error);

  // WindowFunctionExpression used as aggregate function must not be a pure window function.
  const auto window_function = std::make_shared<WindowFunctionExpression>(WindowFunction::Rank, _a);
  EXPECT_THROW(AggregateNode::make(expression_vector(), expression_vector(window_function)), std::logic_error);

  // WindowFunctionExpression used as aggregate function must not define a window.
  auto frame_description = FrameDescription{FrameType::Range, FrameBound{0, FrameBoundType::Preceding, true},
                                            FrameBound{0, FrameBoundType::CurrentRow, false}};
  const auto window =
      window_(expression_vector(), expression_vector(), std::vector<SortMode>{}, std::move(frame_description));

  EXPECT_THROW(AggregateNode::make(expression_vector(), expression_vector(min_(_a, window))), std::logic_error);
}

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
  // identify the difference in the aggregate expressions. That is acceptable, as long as the comparison identifies the
  // two nodes as non-equal.
  EXPECT_NE(_aggregate_node->hash(), different_aggregate_node_c->hash());
  EXPECT_NE(_aggregate_node->hash(), different_aggregate_node_d->hash());
}

TEST_F(AggregateNodeTest, Copy) {
  const auto same_aggregate_node = AggregateNode::make(
      expression_vector(_a, _c), expression_vector(sum_(add_(_a, _b)), sum_(add_(_a, _c))), _mock_node);
  EXPECT_EQ(*_aggregate_node->deep_copy(), *same_aggregate_node);
}

TEST_F(AggregateNodeTest, UniqueColumnCombinationsAdd) {
  EXPECT_TRUE(_mock_node->unique_column_combinations().empty());

  const auto aggregate1 = sum_(add_(_a, _b));
  const auto aggregate2 = sum_(add_(_a, _c));
  const auto agg_node_a =
      AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);
  const auto agg_node_b =
      AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate1, aggregate2), _mock_node);

  // Check whether AggregateNode adds a new UCC for its group-by column(s).
  {
    const auto& unique_column_combinations = agg_node_a->unique_column_combinations();
    EXPECT_EQ(unique_column_combinations.size(), 1);
    EXPECT_TRUE(unique_column_combinations.contains(UniqueColumnCombination{{_a}}));
  }
  {
    const auto& unique_column_combinations = agg_node_b->unique_column_combinations();
    EXPECT_EQ(unique_column_combinations.size(), 1);
    EXPECT_TRUE(unique_column_combinations.contains(UniqueColumnCombination{{_a, _b}}));
  }
}

TEST_F(AggregateNodeTest, UniqueColumnCombinationsForwardingSimple) {
  const auto key_constraint_b = TableKeyConstraint{{_b->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 2);

  const auto aggregate_c = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate_c), _mock_node);
  const auto& unique_column_combinations = _aggregate_node->unique_column_combinations();

  /**
   * Expected behaviour:
   *  - UCC from key_constraint_b remains valid since _b is part of the group-by columns.
   *  - UCC from key_constraint_c, however, should be discarded because _c gets aggregated.
   */

  // Basic check.
  EXPECT_EQ(unique_column_combinations.size(), 1);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_b, unique_column_combinations));
}

TEST_F(AggregateNodeTest, UniqueColumnCombinationsForwardingAnyAggregates) {
  const auto key_constraint_b = TableKeyConstraint{{_b->original_column_id}, KeyConstraintType::UNIQUE};
  const auto key_constraint_c = TableKeyConstraint{{_c->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({key_constraint_b, key_constraint_c});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 2);

  const auto aggregate_b = any_(_b);
  const auto aggregate_c = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a), expression_vector(aggregate_b, aggregate_c), _mock_node);
  const auto& unique_column_combinations = _aggregate_node->unique_column_combinations();

  /**
   * Expected behaviour:
   *  - UCC from key_constraint_b remains valid because _b is aggregated via ANY(), a pseudo aggregate function used by
   *    the DependentGroupByReductionRule to optimize group-bys.
   *  - UCC from key_constraint_c should be discarded because _c is aggregated.
   *  - Also, we should gain a new UCC covering all group-by columns.
   */

  // Basic check.
  EXPECT_EQ(unique_column_combinations.size(), 2);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(key_constraint_b, unique_column_combinations));
  EXPECT_FALSE(find_ucc_by_key_constraint(key_constraint_c, unique_column_combinations));
  const auto ucc_group_by = UniqueColumnCombination{{_a}};
  EXPECT_TRUE(unique_column_combinations.contains(ucc_group_by));
}

TEST_F(AggregateNodeTest, UniqueColumnCombinationsNoDuplicates) {
  // Prepare single UCC.
  const auto table_key_constraint = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_key_constraint});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 1);

  const auto aggregate1 = sum_(_b);
  const auto aggregate2 = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a), expression_vector(aggregate1, aggregate2), _mock_node);

  /**
   * AggregateNode should try to create a new UCC from its group-by-column _a. It is the same as MockNode's UCC, which
   * is forwarded.
   *
   * Expected behaviour: AggregateNode should not output the same UCC twice.
   */

  // Basic check.
  const auto& unique_column_combinations = _aggregate_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(table_key_constraint, unique_column_combinations));
}

TEST_F(AggregateNodeTest, UniqueColumnCombinationsNoSupersets) {
  // Prepare single UCC.
  const auto table_key_constraint = TableKeyConstraint{{_a->original_column_id}, KeyConstraintType::UNIQUE};
  _mock_node->set_key_constraints({table_key_constraint});
  EXPECT_EQ(_mock_node->unique_column_combinations().size(), 1);

  const auto aggregate = sum_(_c);
  _aggregate_node = AggregateNode::make(expression_vector(_a, _b), expression_vector(aggregate), _mock_node);

  /**
   * AggregateNode should try to create a new UCC from both group-by-columns _a and _b. However, MockNode already has a
   * UCC for _a, which is forwarded. It is shorter, and thus preferred over the UCC covering both _a and _b.
   *
   * Expected behaviour: AggregateNode should only forward the input UCC.
   */

  // Basic check.
  const auto& unique_column_combinations = _aggregate_node->unique_column_combinations();
  EXPECT_EQ(unique_column_combinations.size(), 1);
  // In-depth check.
  EXPECT_TRUE(find_ucc_by_key_constraint(table_key_constraint, unique_column_combinations));
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
  EXPECT_TRUE(agg_node_b->non_trivial_functional_dependencies().contains(fd_a));

  // Special case: All determinant expressions, but only some of the dependent expressions are part of the output
  const auto& agg_node_c =
      AggregateNode::make(expression_vector(_b, _c), expression_vector(aggregate1, aggregate2), _mock_node);
  const auto expected_fd = FunctionalDependency{{_b}, {_c}};
  EXPECT_EQ(agg_node_c->non_trivial_functional_dependencies().size(), 1);
  EXPECT_TRUE(agg_node_c->non_trivial_functional_dependencies().contains(expected_fd));
}

TEST_F(AggregateNodeTest, FunctionalDependenciesAdd) {
  // The group-by columns form a new candidate key / UCC from which we should derive a trivial FD.
  _mock_node->set_key_constraints({});
  _mock_node->set_non_trivial_functional_dependencies({});

  const auto& fds = _aggregate_node->functional_dependencies();
  EXPECT_EQ(fds.size(), 1);
  const auto& fd = *fds.cbegin();
  const auto expected_determinants =
      ExpressionUnorderedSet{_group_by_expressions.cbegin(), _group_by_expressions.cend()};
  const auto expected_dependents =
      ExpressionUnorderedSet{_aggregate_expressions.cbegin(), _aggregate_expressions.cend()};
  EXPECT_EQ(fd.determinants, expected_determinants);
  EXPECT_EQ(fd.dependents, expected_dependents);
}

}  // namespace hyrise
