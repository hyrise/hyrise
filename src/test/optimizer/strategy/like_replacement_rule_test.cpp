#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "base_test.hpp"
#include "gtest/gtest.h"

#include "expression/expression_functional.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/lqp_translator.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/projection_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "logical_query_plan/union_node.hpp"
#include "operators/get_table.hpp"
#include "optimizer/strategy/like_replacement_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class LikeReplacementRuleTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    _rule = std::make_shared<LikeReplacementRule>();
    node = MockNode::make(MockNode::ColumnDefinitions{{DataType::String, "a"}});
    a = node->get_column("a");
  }

  std::shared_ptr<MockNode> node;
  LQPColumnReference a;
  std::shared_ptr<LikeReplacementRule> _rule;
};

TEST_F(LikeReplacementRuleTest, LikeReplacement) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED%"), node);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than_(a, "REE"),
    PredicateNode::make(greater_than_equals_(a, "RED"), node));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, LikeReplacementOnNonColumnExpression) {
  const auto input_lqp = PredicateNode::make(like_(concat_(a, a), "RED%"), node);

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than_(concat_(a, a), "REE"),
    PredicateNode::make(greater_than_equals_(concat_(a, a), "RED"), node));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, DoubleWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED%E%"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "RED%E%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, NoWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "RED"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, OnlyWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "%"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, StartsWithWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "%RED"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "%RED"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, MultipleLikes) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED%"), PredicateNode::make(like_(a, "BLUE%"), node));

  // clang-format off
  const auto expected_lqp =
  PredicateNode::make(less_than_(a, "REE"),
    PredicateNode::make(greater_than_equals_(a, "RED"),
        PredicateNode::make(less_than_(a, "BLUF"),
          PredicateNode::make(greater_than_equals_(a, "BLUE"), node))));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, LikeWithUnderscore) {
  const auto input_lqp = PredicateNode::make(like_(a, "R_D%"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "R_D%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, NotLike) {
  const auto input_lqp = PredicateNode::make(not_like_(a, "RED%"), node);

  const auto expected_lqp = PredicateNode::make(not_like_(a, "RED%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, MultipleOutputs) {
  // clang-format off
  const auto like_node = PredicateNode::make(like_(a, "RED%"), node);

  const auto input_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a, "a"),
      like_node),
    PredicateNode::make(less_than_(a, "Z"),
      like_node));

  const auto expected_like_replacement =
  PredicateNode::make(less_than_(a, "REE"),
    PredicateNode::make(greater_than_equals_(a, "RED"),
      node));

  const auto expected_lqp =
  UnionNode::make(UnionMode::Positions,
    PredicateNode::make(greater_than_(a, "a"),
       expected_like_replacement),
    PredicateNode::make(less_than_(a, "Z"),
      expected_like_replacement));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementRuleTest, LastASCIIChar) {
  const auto input_lqp = PredicateNode::make(like_(a, "RE\x7F%"), node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

}  // namespace opossum
