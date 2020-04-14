#include <memory>

#include "base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicatePlacementRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    Hyrise::get().storage_manager.add_table("a", load_table("resources/test_data/tbl/int_float.tbl"));
    _table_a = std::make_shared<StoredTableNode>("a");
    _a_a = LQPColumnReference(_table_a, ColumnID{0});
    _a_b = LQPColumnReference(_table_a, ColumnID{1});

    Hyrise::get().storage_manager.add_table("b", load_table("resources/test_data/tbl/int_float2.tbl"));
    _table_b = std::make_shared<StoredTableNode>("b");
    _b_a = LQPColumnReference(_table_b, ColumnID{0});
    _b_b = LQPColumnReference(_table_b, ColumnID{1});

    Hyrise::get().storage_manager.add_table("c", load_table("resources/test_data/tbl/int_float3.tbl"));
    _table_c = std::make_shared<StoredTableNode>("c");
    _c_a = LQPColumnReference(_table_c, ColumnID{0});
    _c_b = LQPColumnReference(_table_c, ColumnID{1});

    Hyrise::get().storage_manager.add_table("d", load_table("resources/test_data/tbl/int_int3.tbl"));
    _table_d = std::make_shared<StoredTableNode>("d");
    _d_a = LQPColumnReference(_table_d, ColumnID{0});
    _d_b = LQPColumnReference(_table_d, ColumnID{1});

    Hyrise::get().storage_manager.add_table("e", load_table("resources/test_data/tbl/int_int4.tbl"));
    _table_e = std::make_shared<StoredTableNode>("e");
    _e_a = LQPColumnReference(_table_e, ColumnID{0});
    _e_b = LQPColumnReference(_table_e, ColumnID{1});

    _rule = std::make_shared<PredicatePlacementRule>();

    {
      // Initialization of projection pushdown LQP
      const auto int_float_node_a = StoredTableNode::make("a");
      auto a = _table_a->get_column("a");

      auto parameter_c = correlated_parameter_(ParameterID{0}, a);
      auto lqp_c = AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, parameter_c))),
                                       ProjectionNode::make(expression_vector(add_(a, parameter_c)), int_float_node_a));

      _subquery_c = lqp_subquery_(lqp_c, std::make_pair(ParameterID{0}, a));

      _projection_pushdown_node = ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c), _table_a);
    }

    _parameter_a_a = correlated_parameter_(ParameterID{0}, _a_a);
    _subquery_lqp = PredicateNode::make(equals_(_parameter_a_a, _b_a), _table_b);
    _subquery = lqp_subquery_(_subquery_lqp, std::make_pair(ParameterID{0}, _a_a));
  }

  std::shared_ptr<CorrelatedParameterExpression> _parameter_a_a;
  std::shared_ptr<AbstractLQPNode> _subquery_lqp;
  std::shared_ptr<PredicatePlacementRule> _rule;
  std::shared_ptr<StoredTableNode> _table_a, _table_b, _table_c, _table_d, _table_e;
  LQPColumnReference _a_a, _a_b, _b_a, _b_b, _c_a, _c_b, _d_a, _d_b, _e_a, _e_b;
  std::shared_ptr<ProjectionNode> _projection_pushdown_node;
  std::shared_ptr<opossum::LQPSubqueryExpression> _subquery_c, _subquery;
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

  const auto expected_lqp =
  SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(greater_than_(_a_a, _a_b),
      _table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, DiamondPushdownInputRecoveryTest) {
  // If the predicate cannot be pushed down and is effectively re-inserted at the same position, make sure that
  // its outputs are correctly restored.
  // clang-format off
  const auto input_sub_lqp =
  PredicateNode::make(greater_than_(_a_a, 1),
    ValidateNode::make(
      _table_a));

  const auto input_lqp =
  UpdateNode::make("int_float",
    input_sub_lqp,
    ProjectionNode::make(expression_vector(_a_a, cast_(3.2, DataType::Float)),
      input_sub_lqp));

  const auto expected_sub_lqp =
  PredicateNode::make(greater_than_(_a_a, 1),
    ValidateNode::make(
      _table_a));

  const auto expected_lqp =
  UpdateNode::make("int_float",
    expected_sub_lqp,
    ProjectionNode::make(expression_vector(_a_a, cast_(3.2, DataType::Float)),
      expected_sub_lqp));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, StopPushdownAtDiamondTest) {
  // We should stop pushing down predicates once we reached a node with multiple outputs
  // clang-format off
  const auto input_common_node =
  PredicateNode::make(greater_than_(_a_a, 1),
    ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
      _table_a));

  const auto input_lqp =
  UnionNode::make(UnionMode::All,
    PredicateNode::make(greater_than_(_a_a, 2),
      PredicateNode::make(less_than_(_a_b, 5),
       ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(3.2, DataType::Float)),
        input_common_node))),
    PredicateNode::make(greater_than_(_a_a, 10),
      PredicateNode::make(less_than_(_a_b, 50),
       ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(5.2, DataType::Float)),
        input_common_node))));

  const auto expected_common_node =
  PredicateNode::make(greater_than_(_a_a, 1),
    ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
      _table_a));

  const auto expected_lqp =
  UnionNode::make(UnionMode::All,
    ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(3.2, DataType::Float)),
      PredicateNode::make(greater_than_(_a_a, 2),
        PredicateNode::make(less_than_(_a_b, 5),
          expected_common_node))),
    ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(5.2, DataType::Float)),
      PredicateNode::make(greater_than_(_a_a, 10),
        PredicateNode::make(less_than_(_a_b, 50),
          expected_common_node))));
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
  // We can push `a > 4` under the projection because it does not depend on the subquery.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, value_(4)));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, AllowedColumnPredicatePushdownThroughProjectionTest) {
  // We can push `a > b` under the projection because it does not depend on the subquery.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, ForbiddenPredicatePushdownThroughProjectionTest) {
  // We can't push `(SELECT ...) > a.b` under the projection because the projection is responsible for the SELECT.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_subquery_c, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, predicate_node);
  EXPECT_EQ(reordered->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, PredicatePushdownThroughOtherPredicateTest) {
  // Even if one predicate cannot be pushed down, others might be better off

  auto predicate_node_1 = std::make_shared<PredicateNode>(greater_than_(_subquery_c, _a_b));
  predicate_node_1->set_left_input(_projection_pushdown_node);

  auto predicate_node_2 = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node_2->set_left_input(predicate_node_1);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_2);

  EXPECT_EQ(reordered, predicate_node_1);
  EXPECT_EQ(reordered->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input(), predicate_node_2);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePlacementRuleTest, SemiPushDown) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_c_a, 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      PredicateNode::make(greater_than_(_b_b, 123),
        JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
          JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
            _table_b,
            _table_c),
          _table_a))));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
    PredicateNode::make(greater_than_(_b_b, 123),
      JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
        _table_b,
        _table_a)),
    PredicateNode::make(greater_than_(_c_a, 150),
      PredicateNode::make(greater_than_(_c_a, 100),
        _table_c)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PushDownPredicateThroughAggregate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        _table_c)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
      PredicateNode::make(greater_than_(_c_a, 100),
        _table_c)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PushDownAntiThroughAggregate) {
  // clang-format off
  const auto input_lqp =
  JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_c_a, _b_a),
    PredicateNode::make(greater_than_(sum_(_c_b), 150),
      PredicateNode::make(greater_than_(_c_a, 100),
        AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
          _table_c))),
    _table_b);

  const auto expected_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
      JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_c_a, _b_a),
        PredicateNode::make(greater_than_(_c_a, 100),
          _table_c),
        _table_b)));
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
    PredicateNode::make(exists_(_subquery),
      _table_a));

  const auto expected_lqp =
  PredicateNode::make(exists_(_subquery),
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
    PredicateNode::make(exists_(_subquery),
      _table_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_b),
    PredicateNode::make(exists_(_subquery),
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
    PredicateNode::make(exists_(_subquery),
      _table_a));

  const auto expected_lqp =
  SortNode::make(expression_vector(_a_b), std::vector<OrderByMode>{OrderByMode::Ascending},
    PredicateNode::make(exists_(_subquery),
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
  const auto input_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subquery), _table_a);
  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(_subquery),
      input_predicate_node_with_multiple_outputs),
    input_predicate_node_with_multiple_outputs);

  const auto expected_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subquery), _table_a);
  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(exists_(_subquery),
      expected_predicate_node_with_multiple_outputs),
    expected_predicate_node_with_multiple_outputs);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PushDownAndPullUp) {
  // clang-format off
  const auto parameter = correlated_parameter_(ParameterID{0}, _a_a);
  const auto subquery_lqp =
  AggregateNode::make(expression_vector(), expression_vector(max_(add_(_b_a, parameter))),
    ProjectionNode::make(expression_vector(add_(_b_a, parameter)),
      _table_b));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, _a_a));

  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_(_a_a, _a_b),
      PredicateNode::make(less_than_(subquery, _a_b),
        SortNode::make(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending},
           ProjectionNode::make(expression_vector(_a_a, _a_b),
             _table_a)))),
    _table_b);

  const auto expected_lqp =
  PredicateNode::make(less_than_(subquery, _a_b),
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

TEST_F(PredicatePlacementRuleTest, DoNotMoveUncorrelatedPredicates) {
  // For now, the PredicatePlacementRule doesn't touch uncorrelated (think 6 > (SELECT...)) predicates

  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(5, 3),
    PredicateNode::make(equals_(_a_a, 3),
      JoinNode::make(JoinMode::Cross,
      _table_a,
      _table_b)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(5, 3),
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(equals_(_a_a, 3),
        _table_a),
      _table_b));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, CreatePreJoinPredicateOnLeftSide) {
  // SELECT * FROM d JOIN e on d.a = e.a WHERE (d.b = 1 AND e.a = 2) OR (d.b = 2) should lead to
  // (b = 1 OR b = 2) being created on the left side of the join. We cannot filter the right side, because
  // all tuples qualify for the second part of the disjunction.

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), equals_(_d_b, 2)),
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      _table_d,
      _table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), equals_(_d_b, 2)),
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _table_d),
      _table_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, CreatePreJoinPredicateOnBothSides) {
  // SELECT * FROM d JOIN e on d.a = e.a WHERE (d.b = 1 AND e.a = 10) OR (d.b = 2 AND e.a = 3) should lead to
  // (b = 1 OR b = 2) being created on the left and (a = 10 OR a = 3) on the right side of the join

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      _table_d,
      _table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _table_d),
      PredicateNode::make(or_(equals_(_e_a, 10), equals_(_e_a, 1)),
        _table_e)));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, CreatePreJoinPredicateOnlyWhereBeneficial) {
  // SELECT * FROM d JOIN e on d.a = e.a WHERE (d.b = 1 AND e.a < 10) OR (d.b = 2 AND e.a = 3) should lead to
  // (b = 1 OR b = 2) being created on the left side of the join, but no predicate on the right side, as it only
  // removes 2 out of 11 values and thus is not selective enough

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), less_than_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      _table_d,
      _table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), less_than_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _table_d),
      _table_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, DoNotCreatePreJoinPredicateIfNonInner) {
  // Similar to the previous test, but we do not do anything (yet) because it uses a non-inner join

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Left, equals_(_d_a, _e_a),
      _table_d,
      _table_e));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, DoNotCreatePreJoinPredicateIfUnrelated) {
  // SELECT * FROM a JOIN b on a.a = b.a WHERE (a.b = 1 AND ? = 2) OR (b.a = 2 AND ? = 1) should not lead to a pre-join
  // being created, as we cannot make any assumptions about the two predicates that do not belong to any table

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(placeholder_(ParameterID{0}), 2)), and_(equals_(_e_a, 10), equals_(placeholder_(ParameterID{1}), 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      _table_d,
      _table_e));

  const auto expected_lqp = input_lqp->deep_copy();
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
