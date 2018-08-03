#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/aggregate_node.hpp"
#include "logical_query_plan/logical_plan_root_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/predicate_pushdown_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "types.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class PredicatePushdownRuleTest : public StrategyBaseTest {
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

    _rule = std::make_shared<PredicatePushdownRule>();

    {
      // Initialization of projection pushdown LQP
      auto int_float_node_a = StoredTableNode::make("a");
      auto a = LQPColumnReference{int_float_node_a, ColumnID{0}};

      auto parameter_c = parameter_(ParameterID{0}, a);
      auto lqp_c = AggregateNode::make(expression_vector(), expression_vector(max_(add_(a, parameter_c))),
                                       ProjectionNode::make(expression_vector(add_(a, parameter_c)), int_float_node_a));

      _select_c = select_(lqp_c, std::make_pair(ParameterID{0}, a));

      _projection_pushdown_node = ProjectionNode::make(expression_vector(_a_a, _a_b, _select_c), _table_a);
    }
  }

  std::shared_ptr<PredicatePushdownRule> _rule;
  std::shared_ptr<StoredTableNode> _table_a, _table_b, _table_c;
  LQPColumnReference _a_a, _a_b, _b_a, _b_b, _c_a, _c_b;
  std::shared_ptr<ProjectionNode> _projection_pushdown_node;
  std::shared_ptr<opossum::LQPSelectExpression> _select_c;
};

TEST_F(PredicatePushdownRuleTest, SimpleLiteralJoinPushdownTest) {
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

TEST_F(PredicatePushdownRuleTest, SimpleOneSideJoinPushdownTest) {
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

TEST_F(PredicatePushdownRuleTest, SimpleBothSideJoinPushdownTest) {
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

TEST_F(PredicatePushdownRuleTest, SimpleSortPushdownTest) {
  auto sort_node =
      std::make_shared<SortNode>(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending});
  sort_node->set_left_input(_table_a);

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node->set_left_input(sort_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, sort_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, ComplexBlockingPredicatesPushdownTest) {
  auto join_node_ab = std::make_shared<JoinNode>(JoinMode::Inner, equals_(_a_a, _b_a));
  auto join_node_bc = std::make_shared<JoinNode>(JoinMode::Inner, equals_(_b_a, _c_a));

  join_node_bc->set_left_input(_table_b);
  join_node_bc->set_right_input(_table_c);
  join_node_ab->set_left_input(join_node_bc);
  join_node_ab->set_right_input(_table_a);

  auto predicate_node_0 = std::make_shared<PredicateNode>(equals_(_b_b, _a_b));
  predicate_node_0->set_left_input(join_node_ab);

  auto predicate_node_1 = std::make_shared<PredicateNode>(greater_than_(_a_b, 123));
  predicate_node_1->set_left_input(predicate_node_0);

  auto predicate_node_2 = std::make_shared<PredicateNode>(greater_than_(_c_a, 100));
  predicate_node_2->set_left_input(predicate_node_1);

  auto reordered0 = StrategyBaseTest::apply_rule(_rule, predicate_node_2);
  auto reordered1 = StrategyBaseTest::apply_rule(_rule, reordered0);
  auto reordered = StrategyBaseTest::apply_rule(_rule, reordered1);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_input(), join_node_ab);
  EXPECT_EQ(reordered->left_input()->left_input(), join_node_bc);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input(), _table_b);
  EXPECT_EQ(reordered->left_input()->left_input()->right_input(), predicate_node_2);
  EXPECT_EQ(reordered->left_input()->left_input()->right_input()->left_input(), _table_c);
  EXPECT_EQ(reordered->left_input()->right_input(), predicate_node_1);
  EXPECT_EQ(reordered->left_input()->right_input()->left_input(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, AllowedValuePredicatePushdownThroughProjectionTest) {
  // We can push `a > 4` under the projection because it does not depend on the subselect.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, value_(4)));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, AllowedColumnPredicatePushdownThroughProjectionTest) {
  // We can push `a > b` under the projection because it does not depend on the subselect.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input(), predicate_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, ForbiddenPredicatePushdownThroughProjectionTest) {
  // We can't push `(SELECT ...) > a.b` under the projection because the projection is responsible for the SELECT.

  auto predicate_node = std::make_shared<PredicateNode>(greater_than_(_select_c, _a_b));
  predicate_node->set_left_input(_projection_pushdown_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node);

  EXPECT_EQ(reordered, predicate_node);
  EXPECT_EQ(reordered->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, PredicatePushdownThroughOtherPredicateTest) {
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

TEST_F(PredicatePushdownRuleTest, PredicatePushdownThroughMultipleNodesTest) {
  // Even if one predicate cannot be pushed down, others might be better off

  auto sort_node =
      std::make_shared<SortNode>(expression_vector(_a_a), std::vector<OrderByMode>{OrderByMode::Ascending});
  sort_node->set_left_input(_projection_pushdown_node);

  auto predicate_node_1 = std::make_shared<PredicateNode>(greater_than_(_select_c, _a_b));
  predicate_node_1->set_left_input(sort_node);

  auto predicate_node_2 = std::make_shared<PredicateNode>(greater_than_(_a_a, _a_b));
  predicate_node_2->set_left_input(predicate_node_1);

  auto root = std::make_shared<LogicalPlanRootNode>();
  root->set_left_input(predicate_node_2);

  auto reordered = StrategyBaseTest::apply_rule(_rule, root);  // pushes predicate_node_1 under sort
  reordered = StrategyBaseTest::apply_rule(_rule, root);  // pushes predicate_node_2 under sort
  reordered = StrategyBaseTest::apply_rule(_rule, root);  // pushes predicate_node_2 under the projection

  EXPECT_EQ(reordered->left_input(), sort_node);
  EXPECT_EQ(reordered->left_input()->left_input(), predicate_node_1);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input(), _projection_pushdown_node);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input()->left_input(), predicate_node_2);
  EXPECT_EQ(reordered->left_input()->left_input()->left_input()->left_input()->left_input(), _table_a);
}

}  // namespace opossum
