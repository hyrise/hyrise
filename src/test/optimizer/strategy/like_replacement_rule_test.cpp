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

class LikeReplacementTest : public StrategyBaseTest {
 protected:
  void SetUp() override {
    const auto table = load_table("resources/test_data/tbl/string_like_pruning.tbl");
    StorageManager::get().add_table("a", table);
    _rule = std::make_shared<LikeReplacementRule>();
    node = StoredTableNode::make("a");
    a = LQPColumnReference{node, ColumnID{0}};
  }

  std::shared_ptr<StoredTableNode> node;
  LQPColumnReference a;
  std::shared_ptr<LikeReplacementRule> _rule;
};

TEST_F(LikeReplacementTest, LikeReplacement) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED%"), node);

  const auto expected_lqp =
      PredicateNode::make(less_than_(a, "REE"), PredicateNode::make(greater_than_equals_(a, "RED"), node));

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementTest, DoubleWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED%E%"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "RED%E%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementTest, NoWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "RED"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "RED"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementTest, OnlyWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "%"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "%"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

TEST_F(LikeReplacementTest, StartsWithWildcard) {
  const auto input_lqp = PredicateNode::make(like_(a, "%RED"), node);

  const auto expected_lqp = PredicateNode::make(like_(a, "%RED"), node);

  const auto result_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(result_lqp, expected_lqp);
}

}  // namespace opossum
