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
#include "optimizer/strategy/between_composition_rule.hpp"
#include "optimizer/strategy/strategy_base_test.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"
#include "storage/chunk_encoder.hpp"
#include "storage/storage_manager.hpp"

#include "utils/assert.hpp"

#include "logical_query_plan/mock_node.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class BetweenCompositionTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    const auto table = load_table("resources/test_data/tbl/int_int_int.tbl");
    StorageManager::get().add_table("a", table);
    _rule = std::make_shared<BetweenCompositionRule>();
    node = StoredTableNode::make("a");

    a = LQPColumnReference{node, ColumnID{0}};
    b = LQPColumnReference{node, ColumnID{1}};
    c = LQPColumnReference{node, ColumnID{2}};
  }

  std::shared_ptr<StoredTableNode> node;
  LQPColumnReference a, b, c;
  std::shared_ptr<BetweenCompositionRule> _rule;
};

TEST_F(BetweenCompositionTest, ColumnExpressionLeft) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 200),
      PredicateNode::make(
        less_than_equals_(a, 300),
          node));

  const auto expected_lqp = PredicateNode::make(between_(a, 200, 300), node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, ColumnExpressionRight) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      less_than_equals_(200, a),
      PredicateNode::make(
        greater_than_equals_(300, a),
          node));

  const auto expected_lqp = PredicateNode::make(between_(a, 200, 300), node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, NoColumnRange) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 200),
      PredicateNode::make(
        less_than_equals_(b, 300),
          node));

  const auto expected_lqp = input_lqp;
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, ImpossibleColumnRange) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 300),
      PredicateNode::make(
        less_than_equals_(a, 200),
          node));

  const auto expected_lqp = PredicateNode::make(between_(a, 300, 200), node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, LeftExclusive) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_(a, 200),
      PredicateNode::make(
        less_than_equals_(a, 300),
          node));

  const auto expected_lqp = PredicateNode::make(
      std::make_shared<BetweenExpression>(
        a.original_node()->column_expressions()[0],
        value_(200),
        value_(300),
        false,
        true),
      node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, RightExclusive) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 200),
      PredicateNode::make(
        less_than_(a, 300),
          node));

  const auto expected_lqp = PredicateNode::make(
      std::make_shared<BetweenExpression>(
        a.original_node()->column_expressions()[0],
        value_(200),
        value_(300),
        true,
        false),
      node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, BothExclusive) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_(a, 200),
      PredicateNode::make(
        less_than_(a, 300),
          node));

  const auto expected_lqp = PredicateNode::make(
      std::make_shared<BetweenExpression>(
        a.original_node()->column_expressions()[0],
        value_(200),
        value_(300),
        false,
        false),
      node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, FindOptimalBetween) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 100),
      PredicateNode::make(
        greater_than_equals_(a, 200),
        PredicateNode::make(
          less_than_equals_(a, 400),
          PredicateNode::make(
            less_than_equals_(a, 300),
            node))));

  const auto expected_lqp = PredicateNode::make(between_(a, 200, 300), node);
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, KeepRemainingPredicates) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
        greater_than_equals_(a, 200),
        PredicateNode::make(
          less_than_equals_(a, 300),
          PredicateNode::make(
          less_than_equals_(b, 300),
            node)));

  const auto expected_lqp = PredicateNode::make(
    less_than_equals_(b, 300),
    PredicateNode::make(
      between_(a, 200, 300),
      node));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(BetweenCompositionTest, MultipleBetweensVariousLocations) {
  // clang-format off
  const auto input_lqp = PredicateNode::make(
      greater_than_equals_(a, 200),
      PredicateNode::make(
        less_than_equals_(b, 200),
        PredicateNode::make(
          greater_than_equals_(a, 230),
          PredicateNode::make(
            less_than_equals_(c, 200),
            PredicateNode::make(
              less_than_equals_(a, 250),
                PredicateNode::make(
                  less_than_equals_(a, 300),
                  PredicateNode::make(
                    greater_than_equals_(b, 150),
                    node)))))));

  const auto expected_lqp = PredicateNode::make(
        less_than_equals_(c, 200),
        PredicateNode::make(
          between_(a, 230, 250),
          PredicateNode::make(
            between_(b, 150, 200),
            node)));
  // clang-format on

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

}  // namespace opossum
