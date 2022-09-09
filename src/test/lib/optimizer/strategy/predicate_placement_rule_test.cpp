#include <memory>

#include "base_test.hpp"
#include "lib/optimizer/strategy/strategy_base_test.hpp"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "logical_query_plan/update_node.hpp"
#include "logical_query_plan/validate_node.hpp"
#include "optimizer/strategy/predicate_placement_rule.hpp"
#include "types.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class PredicatePlacementRuleTest : public StrategyBaseTest {
 protected:
  static void SetUpTestSuite() {
    _table_a = load_table("resources/test_data/tbl/int_float.tbl");
    _table_b = load_table("resources/test_data/tbl/int_float2.tbl");
    _table_c = load_table("resources/test_data/tbl/int_float3.tbl");
    _table_d = load_table("resources/test_data/tbl/int_int3.tbl");
    _table_e = load_table("resources/test_data/tbl/int_int4.tbl");
  }

  void SetUp() override {
    Hyrise::get().storage_manager.add_table("a", _table_a);
    _stored_table_a = StoredTableNode::make("a");
    _a_a = _stored_table_a->get_column("a");
    _a_b = _stored_table_a->get_column("b");

    Hyrise::get().storage_manager.add_table("b", _table_b);
    _stored_table_b = StoredTableNode::make("b");
    _b_a = _stored_table_b->get_column("a");
    _b_b = _stored_table_b->get_column("b");

    Hyrise::get().storage_manager.add_table("c", _table_c);
    _stored_table_c = StoredTableNode::make("c");
    _c_a = _stored_table_c->get_column("a");
    _c_b = _stored_table_c->get_column("b");

    Hyrise::get().storage_manager.add_table("d", _table_d);
    _stored_table_d = StoredTableNode::make("d");
    _d_a = _stored_table_d->get_column("a");
    _d_b = _stored_table_d->get_column("b");

    Hyrise::get().storage_manager.add_table("e", _table_e);
    _stored_table_e = StoredTableNode::make("e");
    _e_a = _stored_table_e->get_column("a");

    _rule = std::make_shared<PredicatePlacementRule>();

    {
      // Initialization of projection pushdown LQP
      auto parameter_c = correlated_parameter_(ParameterID{0}, _a_a);
      // clang-format off
      auto lqp_c =
        AggregateNode::make(expression_vector(), expression_vector(max_(add_(_a_a, parameter_c))),
          ProjectionNode::make(expression_vector(add_(_a_a, parameter_c)), _stored_table_a));
      // clang-format on
      _subquery_c = lqp_subquery_(lqp_c, std::make_pair(ParameterID{0}, _a_a));
    }

    const auto parameter_a_a = correlated_parameter_(ParameterID{0}, _a_a);
    const auto subquery_lqp = PredicateNode::make(equals_(parameter_a_a, _b_a), _stored_table_b);
    _subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, _a_a));
  }

  inline static std::shared_ptr<Table> _table_a, _table_b, _table_c, _table_d, _table_e;
  std::shared_ptr<PredicatePlacementRule> _rule;
  std::shared_ptr<StoredTableNode> _stored_table_a, _stored_table_b, _stored_table_c, _stored_table_d, _stored_table_e;
  std::shared_ptr<LQPColumnExpression> _a_a, _a_b, _b_a, _b_b, _c_a, _c_b, _d_a, _d_b, _e_a;
  std::shared_ptr<hyrise::LQPSubqueryExpression> _subquery_c, _subquery;
};

TEST_F(PredicatePlacementRuleTest, SimpleLiteralJoinPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, 10),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
      _stored_table_a,
      _stored_table_b));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_(_a_a, 10),
      _stored_table_a),
    _stored_table_b);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, SimpleOneSideJoinPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _a_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
      _stored_table_a,
      _stored_table_b));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_(_a_a, _a_b),
      _stored_table_a),
    _stored_table_b);
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, SimpleBothSideJoinPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _b_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_b, _b_a),
      _stored_table_a,
      _stored_table_b));
  // clang-format on
  const auto expected_lqp = input_lqp->deep_copy();

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(input_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, SimpleSortPushdownTest) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _a_b),
    SortNode::make(expression_vector(_a_a), std::vector<SortMode>{SortMode::Ascending},
      _stored_table_a));

  const auto expected_lqp =
  SortNode::make(expression_vector(_a_a), std::vector<SortMode>{SortMode::Ascending},
    PredicateNode::make(greater_than_(_a_a, _a_b),
      _stored_table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, SimpleDiamondPushdownTest) {
  // We expect predicates to get pushed below UnionNode-based diamonds if they continue to be evaluable.
  // clang-format off
  const auto input_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    _stored_table_a);

  const auto input_lqp =
  PredicateNode::make(equals_(_a_b, 10),    // <-- Predicate before pushdown
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(like_(_a_a, "%man%"),
        input_common_node),
      PredicateNode::make(like_(_a_a, "%Man%"),
        input_common_node)));

  const auto expected_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    PredicateNode::make(equals_(_a_b, 10),  // <-- Predicate after pushdown
      _stored_table_a));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(like_(_a_a, "%man%"),
      expected_common_node),
    PredicateNode::make(like_(_a_a, "%Man%"),
      expected_common_node));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, BlockSimpleDiamondPushdownTest) {
  // Derived from SimpleDiamondPushdownTest. In this test, the diamond's origin node is used by another LQP node,
  // which is not part of the diamond. As a result, the predicate pushdown must be blocked because it would incorrectly
  // filter the other LQP node not part of the diamond.
  // clang-format off
  const auto input_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    _stored_table_a);

  const auto input_lqp =
  PredicateNode::make(equals_(_a_b, 10),  // <-- Predicate, which should NOT get pushed through the diamond
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(like_(_a_a, "%man%"),  // <-- Predicate before pushdown
        ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
          input_common_node)),
      PredicateNode::make(like_(_a_a, "%Man%"),  // <-- Predicate before pushdown
        ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
          input_common_node))));

  // Increase the output count of input_common_node
  ASSERT_EQ(input_common_node->outputs().size(), 2);
  const auto non_diamond_lqp_node = ProjectionNode::make(expression_vector(_a_a), input_common_node);
  ASSERT_EQ(input_common_node->outputs().size(), 3);

  // Predicates are not pushed through the diamond. However, predicates inside the diamond are pushed towards the
  // diamond's origin.
  const auto expected_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    _stored_table_a);

  const auto expected_lqp =
  PredicateNode::make(equals_(_a_b, 10),
    UnionNode::make(SetOperationMode::Positions,
      ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
        PredicateNode::make(like_(_a_a, "%man%"),  // <-- Predicate after pushdown
          expected_common_node)),
      ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
        PredicateNode::make(like_(_a_a, "%Man%"),  // <-- Predicate after pushdown
          expected_common_node))));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PartialDiamondPushdownTest) {
  // Derived from SimpleDiamondPushdownTest. In this test, the diamond's origin node is an AggregateNode which
  // blocks one predicate from getting pushed below the diamond.
  // clang-format off
  const auto input_common_node =
  AggregateNode::make(expression_vector(_a_a), expression_vector(min_(_a_b)),
    _stored_table_a);

  const auto input_lqp =
  PredicateNode::make(equals_(_a_a, 10),                 // <-- 1st Predicate before pushdown
    PredicateNode::make(greater_than_(min_(_a_b), 100),  // <-- 2nd Predicate before pushdown
      UnionNode::make(SetOperationMode::Positions,
        PredicateNode::make(like_(_a_a, "%man%"),
          input_common_node),
        PredicateNode::make(like_(_a_a, "%Man%"),
          input_common_node))));

  const auto expected_common_node =
  PredicateNode::make(greater_than_(min_(_a_b), 100),   // <-- 2nd Predicate after pushdown
    AggregateNode::make(expression_vector(_a_a), expression_vector(min_(_a_b)),
      PredicateNode::make(equals_(_a_a, 10),            // <-- 1st Predicate after pushdown
        _stored_table_a)));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(like_(_a_a, "%man%"),
      expected_common_node),
    PredicateNode::make(like_(_a_a, "%Man%"),
      expected_common_node));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, ConsecutiveDiamondPushdownTest) {
  // In this test, two predicates sit on top of two consecutive Union diamonds. While both predicates can be pushed
  // below the first diamond, only one predicate can also be pushed below the second diamond.
  // clang-format off
  const auto input_common_node =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(like_(_c_a, "%woman%"),
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        _stored_table_c)),
    PredicateNode::make(like_(_c_a, "%Woman%"),
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        _stored_table_c)));

  const auto input_lqp =
  PredicateNode::make(equals_(_c_a, 10),                                  // <-- 1st Predicate before pushdown
    PredicateNode::make(greater_than_(sum_(_c_b), 1000),                  // <-- 2nd Predicate before pushdown
      UnionNode::make(SetOperationMode::Positions,
        PredicateNode::make(like_(_c_a, "%man%"),
          input_common_node),
        PredicateNode::make(like_(_c_a, "%Man%"),
          input_common_node))));

  // We apply the rule before defining the expected LQP. Otherwise, we would modify the outputs of _stored_table_c, and
  // thus affect the rule's outcome.
  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  const auto expected_common_node2 =
  PredicateNode::make(equals_(_c_a, 10),                                // <-- 1st Predicate after pushdown
    _stored_table_c);

  const auto expected_common_node1 =
  PredicateNode::make(greater_than_(sum_(_c_b), 1000),                  // <-- 2nd Predicate after pushdown
    UnionNode::make(SetOperationMode::All,
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        PredicateNode::make(like_(_c_a, "%woman%"),
          expected_common_node2)),
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        PredicateNode::make(like_(_c_a, "%Woman%"),
          expected_common_node2))));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(like_(_c_a, "%man%"),
      expected_common_node1),
    PredicateNode::make(like_(_c_a, "%Man%"),
      expected_common_node1));
  // clang-format on

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, BigDiamondPushdown) {
  // We expect predicates to get pushed below UnionNode-based diamonds if they continue to be evaluable.
  // clang-format off
  const auto input_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    _stored_table_a);

  const auto input_lqp =
  PredicateNode::make(equals_(_a_b, 10),  // <-- Predicate before pushdown
    UnionNode::make(SetOperationMode::Positions,
      UnionNode::make(SetOperationMode::Positions,
        PredicateNode::make(like_(_a_a, "%man"),
          input_common_node),
        PredicateNode::make(like_(_a_a, "%child"),
          input_common_node)),
      PredicateNode::make(like_(_a_a, "%woman"),
          input_common_node)));

  const auto expected_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    PredicateNode::make(equals_(_a_b, 10),  // <-- Predicate after pushdown
      _stored_table_a));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
    UnionNode::make(SetOperationMode::Positions,
      PredicateNode::make(like_(_a_a, "%man"),
        expected_common_node),
      PredicateNode::make(like_(_a_a, "%child"),
        expected_common_node)),
    PredicateNode::make(like_(_a_a, "%woman"),
        expected_common_node));
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
      _stored_table_a));

  const auto input_lqp =
  UpdateNode::make("int_float",
    input_sub_lqp,
    ProjectionNode::make(expression_vector(_a_a, cast_(3.2, DataType::Float)),
      input_sub_lqp));

  const auto expected_sub_lqp =
  PredicateNode::make(greater_than_(_a_a, 1),
    ValidateNode::make(
      _stored_table_a));

  const auto expected_lqp =
  UpdateNode::make("int_float",
    expected_sub_lqp,
    ProjectionNode::make(expression_vector(_a_a, cast_(3.2, DataType::Float)),
      expected_sub_lqp));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, StopPushdownAtUnion) {
  // Stop pushdown at UnionNodes, if they do not form diamonds.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(equals_(_a_b, 10),
    UnionNode::make(SetOperationMode::All,
      PredicateNode::make(less_than_(_a_a, 0),
        _stored_table_a),
      PredicateNode::make(greater_than_(_b_a, 10),
        _stored_table_b)));
  // clang-format on

  const auto expected_lqp = input_lqp->deep_copy();

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, StopPushdownAtDiamondOriginNode) {
  // We must stop pushing down predicates when reaching a node with multiple outputs.
  // clang-format off
  const auto input_common_node =
    ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
      PredicateNode::make(greater_than_(_a_a, 1),
      _stored_table_a));

  const auto input_lqp =
  UnionNode::make(SetOperationMode::All,
    PredicateNode::make(greater_than_(_a_a, 2),
      PredicateNode::make(less_than_(_a_b, 5),
       ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(3.2, DataType::Float)),
        input_common_node))),
    PredicateNode::make(greater_than_(_a_a, 10),
      PredicateNode::make(less_than_(_a_b, 50),
       ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(5.2, DataType::Float)),
        input_common_node))));

  // We expect the diamond predicates to get pushed below the Projections. However, since the diamond's origin node
  // has multiple outputs, predicates are not expected to get pushed down any further.
  const auto expected_common_node =
  ProjectionNode::make(expression_vector(_a_a, _a_b, cast_(11, DataType::Float)),
    PredicateNode::make(greater_than_(_a_a, 1),
      _stored_table_a));

  const auto expected_lqp =
  UnionNode::make(SetOperationMode::All,
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
              _stored_table_b,
              _stored_table_c),
            _stored_table_a)))));

  const auto expected_lqp =
  PredicateNode::make(equals_(_b_b, _a_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
      JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
        _stored_table_b,
        PredicateNode::make(greater_than_(_c_a, 150),
          PredicateNode::make(greater_than_(_c_a, 100),
            _stored_table_c))),
      PredicateNode::make(greater_than_(_a_b, 123),
        _stored_table_a)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, AllowedValuePredicatePushdownThroughProjectionTest) {
  // We can push `a > 4` under the projection because it does not depend on the sub-query.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, value_(4)),
    ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
      _stored_table_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
    PredicateNode::make(greater_than_(_a_a, value_(4)),
       _stored_table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, AllowedColumnPredicatePushdownThroughProjectionTest) {
  // We can push `a > b` under the projection because it does not depend on the sub-query.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _a_b),
    ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
      _stored_table_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
    PredicateNode::make(greater_than_(_a_a, _a_b),
       _stored_table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, ForbiddenPredicatePushdownThroughProjectionTest) {
  // We can't push `(SELECT ...) > a.b` under the projection because the projection is responsible for the SELECT.
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_subquery_c, _a_b),
    ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
      _stored_table_a));
  // clang-format on
  const auto expected_lqp = input_lqp->deep_copy();

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, PredicatePushdownThroughOtherPredicateTest) {
  // Even if one predicate cannot be pushed down, others might be better off
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_a_a, _a_b),
    PredicateNode::make(greater_than_(_subquery_c, _a_b),
      ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
        _stored_table_a)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(_subquery_c, _a_b),
     ProjectionNode::make(expression_vector(_a_a, _a_b, _subquery_c),
        PredicateNode::make(greater_than_(_a_a, _a_b),
          _stored_table_a)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, SemiPushDown) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(_c_a, 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      PredicateNode::make(greater_than_(_b_b, 123),
        JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
          JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
            _stored_table_b,
            _stored_table_c),
          _stored_table_a))));

  const auto expected_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
    PredicateNode::make(greater_than_(_b_b, 123),
      JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),
        _stored_table_b,
        _stored_table_a)),
    PredicateNode::make(greater_than_(_c_a, 150),
      PredicateNode::make(greater_than_(_c_a, 100),
        _stored_table_c)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, HandleBarrierPredicatePushdown) {
  // In this test, the PredicatePlacementRule cannot push down the top two predicates because of a predicate having
  // multiple outputs (the barrier node). The purpose of this test is to check whether a barrier node becomes
  // pushed down by the rule, if it is a predicate eligible for pushdown.

  // clang-format off
  const auto barrier_predicate_node =
  PredicateNode::make(greater_than_(_b_b, 123),                 // <-- 1st Predicate before pushdown (pushdown barrier due to output_count() == 2) // NOLINT
    JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),         // <-- 2nd Predicate before pushdown
      JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
        _stored_table_b,
        _stored_table_c),
      _stored_table_a));

  auto input_lqp =
  PredicateNode::make(greater_than_(_c_a, 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      barrier_predicate_node));

  // Increase the predicate's output count so that it becomes an actual barrier in the _push_down_traversal subroutine.
  const auto temporary_node = LogicalPlanRootNode::make(barrier_predicate_node);
  ASSERT_EQ(barrier_predicate_node->output_count(), 2);

  const auto expected_lqp =
  PredicateNode::make(greater_than_(_c_a, 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      JoinNode::make(JoinMode::Inner, equals_(_b_a, _c_a),
        PredicateNode::make(greater_than_(_b_b, 123),           // <-- 1st Predicate after pushdown
          JoinNode::make(JoinMode::Semi, equals_(_a_a, _b_a),   // <-- 2nd Predicate after pushdown
            _stored_table_b,
            _stored_table_a)),
        _stored_table_c)));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
  EXPECT_EQ(barrier_predicate_node->output_count(), 1);
  // After the pushdown, the inner join should have the two outputs that barrier_predicate_node previously had.
  const auto& inner_join = std::dynamic_pointer_cast<JoinNode>(actual_lqp->left_input()->left_input());
  EXPECT_EQ(inner_join->output_count(), 2);
  EXPECT_EQ(temporary_node->left_input(), inner_join);
}

TEST_F(PredicatePlacementRuleTest, PushDownPredicateThroughAggregate) {
  // clang-format off
  const auto input_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    PredicateNode::make(greater_than_(_c_a, 100),
      AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
        _stored_table_c)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
      PredicateNode::make(greater_than_(_c_a, 100),
        _stored_table_c)));
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
          _stored_table_c))),
    _stored_table_b);

  const auto expected_lqp =
  PredicateNode::make(greater_than_(sum_(_c_b), 150),
    AggregateNode::make(expression_vector(_c_a), expression_vector(sum_(_c_b)),
      JoinNode::make(JoinMode::AntiNullAsTrue, equals_(_c_a, _b_a),
        PredicateNode::make(greater_than_(_c_a, 100),
          _stored_table_c),
        _stored_table_b)));
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
      _stored_table_a));

  const auto expected_lqp =
  PredicateNode::make(exists_(_subquery),
    ProjectionNode::make(expression_vector(_a_a),
      _stored_table_a));
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
      _stored_table_a));

  const auto expected_lqp =
  ProjectionNode::make(expression_vector(_a_b),
    PredicateNode::make(exists_(_subquery),
      _stored_table_a));
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
  SortNode::make(expression_vector(_a_b), std::vector<SortMode>{SortMode::Ascending},
    PredicateNode::make(exists_(_subquery),
      _stored_table_a));

  const auto expected_lqp =
  SortNode::make(expression_vector(_a_b), std::vector<SortMode>{SortMode::Ascending},
    PredicateNode::make(exists_(_subquery),
      _stored_table_a));
  // clang-format on

  auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, NoPullUpPastNodeWithMultipleOutputsNoPullUpPastUnion) {
  /**
   * Test that Nodes with multiple outputs are treated as barriers, and so are UnionNodes
   */
  // clang-format off
  const auto input_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subquery), _stored_table_a);
  const auto input_lqp =
  UnionNode::make(SetOperationMode::Positions,
    PredicateNode::make(exists_(_subquery),
      input_predicate_node_with_multiple_outputs),
    input_predicate_node_with_multiple_outputs);

  const auto expected_predicate_node_with_multiple_outputs = PredicateNode::make(exists_(_subquery), _stored_table_a);
  const auto expected_lqp =
  UnionNode::make(SetOperationMode::Positions,
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
      _stored_table_b));
  const auto subquery = lqp_subquery_(subquery_lqp, std::make_pair(ParameterID{0}, _a_a));

  const auto input_lqp =
  JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
    PredicateNode::make(greater_than_(_a_a, _a_b),
      PredicateNode::make(less_than_(subquery, _a_b),
        SortNode::make(expression_vector(_a_a), std::vector<SortMode>{SortMode::Ascending},
           ProjectionNode::make(expression_vector(_a_a, _a_b),
             _stored_table_a)))),
    _stored_table_b);

  const auto expected_lqp =
  PredicateNode::make(less_than_(subquery, _a_b),
    JoinNode::make(JoinMode::Inner, equals_(_a_a, _b_a),
     SortNode::make(expression_vector(_a_a), std::vector<SortMode>{SortMode::Ascending},
       ProjectionNode::make(expression_vector(_a_a, _a_b),
         PredicateNode::make(greater_than_(_a_a, _a_b),
           _stored_table_a))),
     _stored_table_b));
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
      _stored_table_a,
      _stored_table_b)));

  const auto expected_lqp =
  PredicateNode::make(greater_than_(5, 3),
    JoinNode::make(JoinMode::Cross,
      PredicateNode::make(equals_(_a_a, 3),
        _stored_table_a),
      _stored_table_b));
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
      _stored_table_d,
      _stored_table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), equals_(_d_b, 2)),
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _stored_table_d),
      _stored_table_e));
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
      _stored_table_d,
      _stored_table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _stored_table_d),
      PredicateNode::make(or_(equals_(_e_a, 10), equals_(_e_a, 1)),
        _stored_table_e)));
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
      _stored_table_d,
      _stored_table_e));

  const auto expected_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), less_than_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      PredicateNode::make(or_(equals_(_d_b, 1), equals_(_d_b, 2)),
        _stored_table_d),
      _stored_table_e));
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, DoNotCreatePreJoinPredicateIfNonInner) {
  // Similar to the previous test, but we do not do anything (yet) because it uses a non-inner join

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(_e_a, 10)), and_(equals_(_d_b, 2), equals_(_e_a, 1))),  // NOLINT
    JoinNode::make(JoinMode::Left, equals_(_d_a, _e_a),
      _stored_table_d,
      _stored_table_e));
  // clang-format on
  const auto expected_lqp = input_lqp->deep_copy();

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(PredicatePlacementRuleTest, DoNotCreatePreJoinPredicateIfUnrelated) {
  // SELECT * FROM a JOIN b on a.a = b.a WHERE (a.b = 1 AND ? = 2) OR (b.a = 2 AND ? = 1) should not lead to a pre-join
  // being created, as we cannot make any assumptions about the two predicates that do not belong to any table

  // clang-format off
  const auto input_lqp = PredicateNode::make(or_(and_(equals_(_d_b, 1), equals_(placeholder_(ParameterID{0}), 2)), and_(equals_(_e_a, 10), equals_(placeholder_(ParameterID{1}), 1))),  // NOLINT
    JoinNode::make(JoinMode::Inner, equals_(_d_a, _e_a),
      _stored_table_d,
      _stored_table_e));
  // clang-format on
  const auto expected_lqp = input_lqp->deep_copy();

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace hyrise
