#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "testing_assert.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicatePlacementRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    StorageManager::get().add_table("a", load_table("src/test/tables/int_float.tbl", Chunk::MAX_SIZE));
    _table_a = std::make_shared<StoredTableNode>("a");
    _a_a = LQPColumnReference(_table_a, ColumnID{0});
    _a_b = LQPColumnReference(_table_a, ColumnID{1});

    StorageManager::get().add_table("b", load_table("src/test/tables/int_float2.tbl", Chunk::MAX_SIZE));
    _table_b = std::make_shared<StoredTableNode>("b");
    _b_a = LQPColumnReference(_table_b, ColumnID{0});
    _b_b = LQPColumnReference(_table_b, ColumnID{1});

    StorageManager::get().add_table("c", load_table("src/test/tables/int_float3.tbl", Chunk::MAX_SIZE));
    _table_c = std::make_shared<StoredTableNode>("c");
    _c_a = LQPColumnReference(_table_c, ColumnID{0});
    _c_b = LQPColumnReference(_table_c, ColumnID{1});

    _rule = std::make_shared<PredicatePlacementRule>();

    {
      // Initialization of projection pushdown LQP
      auto int_float_node_a = StoredTableNode::make("a");
      auto a = LQPColumnReference{int_float_node_a, ColumnID{0}};

      auto parameter_c = correlated_parameter_(ParameterID{0}, a);
      auto lqp_c = AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, parameter_c))),
                                       ProjectionNode::make(expression_vector(add_(a, parameter_c)), int_float_node_a));

      _select_c = lqp_select_(lqp_c, std::make_pair(ParameterID{0}, a));

      _projection_pushdown_node = ProjectionNode::make(expression_vector(_a_a, _a_b, _select_c), _table_a);
    }

    _parameter_a_a = correlated_parameter_(ParameterID{0}, _a_a);
    _subselect_lqp = PredicateNode::make(equals_(_parameter_a_a, _b_a), _table_b);
    _subselect = lqp_select_(_subselect_lqp, std::make_pair(ParameterID{0}, _a_a));
  }

  std::shared_ptr<ParameterExpression> _parameter_a_a;
  std::shared_ptr<AbstractLQPNode> _subselect_lqp;
  std::shared_ptr<PredicatePlacementRule> _rule;
  std::shared_ptr<StoredTableNode> _table_a, _table_b, _table_c;
  LQPColumnReference _a_a, _a_b, _b_a, _b_b, _c_a, _c_b;
  std::shared_ptr<ProjectionNode> _projection_pushdown_node;
  std::shared_ptr<opossum::LQPSelectExpression> _select_c, _subselect;
};

TEST_F(PredicatePlacementRuleTest, SimpleLiteralJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, equals_(_a_a, _b_a));
  join_node->set_left_input(_table_a);
  join_node->set_right_input(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(greater_than_(_a_a, 10));
  predicate_node_0->set_left_input(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, join_node);
  EXPECT_EQ(reordered->left_input(), predicate_node_0);
  EXPECT_EQ(reordered->right_input(), _table_b);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, SimpleOneSideJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, equals_(_a_a, _b_a));
  join_node->set_left_input(_table_a);
  join_node->set_right_input(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node_0->set_left_input(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, join_node);
  EXPECT_EQ(reordered->left_input(), predicate_node_0);
  EXPECT_EQ(reordered->right_input(), _table_b);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, SimpleBothSideJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, equals_(_a_b, _b_a));
  join_node->set_left_input(_table_a);
  join_node->set_right_input(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(greater_than_(_a_a, _b_b));
  predicate_node_0->set_left_input(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_input(), join_node);
  EXPECT_EQ(reordered->left_input()->right_input(), _table_b);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, SimpleSortPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _a_b),
    SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
      _table_a));
  // clang-format on

  // clang-format off
  const auto expected_lqp =
  SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(greater_than_(_a_a, _a_b),
      _table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, ComplexBlockingPredicatesPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_c_a, 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      PredicateNode::make(greater_than_(_a_b, 123),
        PredicateNode::make(equals_(_b_b, _a_b),
          JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
            JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
              _table_b,
              _table_c),
            _table_a)))));

  const auto expected_lqp =
  PredicateNode::make(equals_(_b_b, _a_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
      JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
        _table_b,
        PredicateNode::make(greater_than_(_c_a, 150),
          PredicateNode::make(greater_than_(_c_a, 100),
            _table_c))),
      PredicateNode::make(greater_than_(_a_b, 123),
        _table_a)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, AllowedValuePredicatePushdownThroughProjectionTest) {
  // We can push `a > 4` under the projection because it does not depend on the subselect.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, value_(4)));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, AllowedColumnPredicatePushdownThroughProjectionTest) {
  // We can push `a > b` under the projection because it does not depend on the subselect.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, ForbiddenPredicatePushdownThroughProjectionTest) {
  // We can't push `(SELECT ...) > a.b` under the projection because the projection is responsible for the SELECT.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_select_c, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, predicate_node);
  EXPECT_EQ(reordered->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, PredicatePushdownThroughOtherPredicateTest) {
  // Even if one predicate cannot be pushed down, others might be better off

  auto predicate_node_1 = std::make_shared<PredicateNode>(greater_than_(_select_c, _a_b));
  predicate_node_1->set_left_input(_projection_pushdown_node);

  auto predicate_node_2 = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node_2->set_left_input(predicate_node_1);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_2);

  EXPECT_EQ(reordered, predicate_node_1);
  EXPECT_EQ(reordered->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input(), predicate_node_2);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, MovePastInnerSemiAntiCrossJoin) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(less_than_(_a_a, 5),
    JoinNode::make(JoinMode::Semi, equals_(_c_a, _a_a),
      JoinNode::make(JoinMode::Anti, equals_(_c_a, _a_a),
        JoinNode::make(JoinMode::Cross,
          JoinNode::make(JoinMode::Inner, equals_(_c_a, _a_a),
            PredicateNode::make(exists_(_subselect),
              _table_a),
            _table_c),
          StoredTableNode::make("c")),
        StoredTableNode::make("c")),
      StoredTableNode::make("c")));

  const auto expected_lqp =
  PredicateNode::make(exists_(_subselect),
    JoinNode::make(JoinMode::Semi, equals_(_c_a, _a_a),
      JoinNode::make(JoinMode::Anti, equals_(_c_a, _a_a),
        JoinNode::make(JoinMode::Cross,
          JoinNode::make(JoinMode::Inner, equals_(_c_a, _a_a),
            PredicateNode::make(less_than_(_a_a, 5),
              _table_a),
            _table_c),
          StoredTableNode::make("c")),
        StoredTableNode::make("c")),
      StoredTableNode::make("c")));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PullUpPastProjection) {
  /**
   * Test that if all columns required for the evaluation of a PredicateNode are still available above a ProjectionNode,
   * then the PredicateNode can be pulled up
   */
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_a),
    PredicateNode::make(exists_(_subselect),
      _table_a));

  const auto expected_lqp =
  PredicateNode::make(exists_(_subselect),
    ProjectionNode::make(expression_vector(_a_a),
      _table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, NoPullUpPastProjectionThatPrunes) {
  /**
   * Test that if a ProjectionNode prunes columns necessary for the evaluation of a PredicateNode, PredicateNodes can
   * not be pulled above it
   */
  // clang-format off
  const auto input_lqp =
  ProjectionNode::make(expression_vector(_a_b),
    PredicateNode::make(exists_(_subselect),
      _table_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_b),
    PredicateNode::make(exists_(_subselect),
      _table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, NoPullUpPastSort) {
  /**
   * Test that SortNodes are treated as barriers and nothing is pulled up above them
   */
  // clang-format off
  const auto input_lqp =
  SortNode::make(expression_vector(_a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(exists_(_subselect),
      _table_a));

  const auto expected_lqp =
  SortNode::make(expression_vector(_a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(exists_(_subselect),
      _table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, NoPullUpPastNodeWithMultipleOutputsNoPullUpPastUnion) {
  /**
   * Test that Nodes with multiple outputs are treated as barriers, and so are UnionNodes
   */
  // clang-format off
  const auto input_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subselect), _table_a);
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(_subselect),
      input_predicate_node_with_multiple_outputs),
    input_predicate_node_with_multiple_outputs);

  const auto expected_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subselect), _table_a);
  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(_subselect),
      expected_predicate_node_with_multiple_outputs),
    expected_predicate_node_with_multiple_outputs);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PushDownAndPullUp) {
  // clang-format off
  const auto parameter = correlated_parameter_(ParameterID{0}, _a_a);
  const auto subselect_lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(_b_a, parameter))),
    ProjectionNode::make(expression_vector(add_(_b_a, parameter)),
      _table_b));
  const auto subselect = lqp_select_(subselect_lqp, std::make_pair(ParameterID{0}, _a_a));

  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_(_a_a, _a_b),
      PredicateNode::make(less_than_(subselect, _a_b),
        SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
           ProjectionNode::make(expression_vector(_a_a, _a_b),
             _table_a)))),
    _table_b);

  const auto expected_lqp =
  PredicateNode::make(less_than_(subselect, _a_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
     SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
       ProjectionNode::make(expression_vector(_a_a, _a_b),
         PredicateNode::make(greater_than_(_a_a, _a_b),
           _table_a))),
     _table_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
