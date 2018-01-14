#include <memory>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/predicate_pushdown_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "types.hpp"

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

    _rule = std::make_shared<PredicatePushdownRule>();
  }

  std::shared_ptr<PredicatePushdownRule> _rule;
  std::shared_ptr<StoredTableNode> _table_a, _table_b;
  LQPColumnReference _a_a, _a_b, _b_a, _b_b;
};

TEST_F(PredicatePushdownRuleTest, SimpleLiteralJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(_a_a, _b_a), PredicateCondition::Equals);
  join_node->set_left_child(_table_a);
  join_node->set_right_child(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(_a_a, PredicateCondition::GreaterThan, 10);
  predicate_node_0->set_left_child(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, join_node);
  EXPECT_EQ(reordered->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->right_child(), _table_b);
  EXPECT_EQ(reordered->left_child()->left_child(), _table_a);
}

TEST_F(PredicatePushdownRuleTest, SimpleOneSideJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(_a_a, _b_a), PredicateCondition::Equals);
  join_node->set_left_child(_table_a);
  join_node->set_right_child(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(_a_a, PredicateCondition::GreaterThan, _a_b);
  predicate_node_0->set_left_child(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, join_node);
  EXPECT_EQ(reordered->left_child(), predicate_node_0);
  EXPECT_EQ(reordered->right_child(), _table_b);
  EXPECT_EQ(reordered->left_child()->left_child(), _table_a);
}


TEST_F(PredicatePushdownRuleTest, SimpleBothSideJoinPushdownTest) {
  auto join_node = std::make_shared<JoinNode>(JoinMode::Inner, std::make_pair(_a_b, _b_a), PredicateCondition::Equals);
  join_node->set_left_child(_table_a);
  join_node->set_right_child(_table_b);

  auto predicate_node_0 = std::make_shared<PredicateNode>(_a_a, PredicateCondition::GreaterThan, _b_b);
  predicate_node_0->set_left_child(join_node);

  auto reordered = StrategyBaseTest::apply_rule(_rule, predicate_node_0);

  EXPECT_EQ(reordered, predicate_node_0);
  EXPECT_EQ(reordered->left_child(), join_node);
  EXPECT_EQ(reordered->left_child()->right_child(), _table_b);
  EXPECT_EQ(reordered->left_child()->left_child(), _table_a);
}

}  // namespace opossum
