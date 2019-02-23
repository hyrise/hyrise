#include "gtest/gtest.h"

#include "strategy_base_test.hpp"
#include "testing_assert.hpp"

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/limit_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/insert_limit_in_exists_rule.hpp"
#include "storage/storage_manager.hpp"
#include "utils/load_table.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class ExistsInsertLimitInExistsRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    StorageManager::get().add_table("table_a", load_table("resources/test_data/tbl/int_int2.tbl"));
    StorageManager::get().add_table("table_b", load_table("resources/test_data/tbl/int_int3.tbl"));

    node_table_a = StoredTableNode::make("table_a");
    node_table_a_col_a = node_table_a->get_column("a");

    node_table_b = StoredTableNode::make("table_b");
    node_table_b_col_a = node_table_b->get_column("a");

    _rule = std::make_shared<InsertLimitInExistsRule>();
  }

  std::shared_ptr<InsertLimitInExistsRule> _rule;

  std::shared_ptr<StoredTableNode> node_table_a, node_table_b;
  LQPColumnReference node_table_a_col_a, node_table_b_col_a;
};

TEST_F(ExistsInsertLimitInExistsRuleTest, AddLimitInSimpleExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto subselect = lqp_subquery_(subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto input_lqp = PredicateNode::make(exists_(subselect), node_table_a);

  const auto limit_subselect_lqp = LimitNode::make(std::make_shared<ValueExpression>(int64_t{1}), subselect_lqp);
  const auto limit_subselect = lqp_subquery_(limit_subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto expected_lqp = PredicateNode::make(exists_(limit_subselect), node_table_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(ExistsInsertLimitInExistsRuleTest, DoNotAddLimitIfLimitExists) {
  const auto parameter = correlated_parameter_(ParameterID{0}, node_table_a_col_a);

  // clang-format off
  const auto subselect_lqp = PredicateNode::make(equals_(node_table_b_col_a, parameter), node_table_b);
  const auto limit_subselect_lqp = LimitNode::make(std::make_shared<ValueExpression>(int64_t{2}), subselect_lqp);
  const auto limit_subselect = lqp_subquery_(limit_subselect_lqp, std::make_pair(ParameterID{0}, node_table_a_col_a));
  const auto lqp = PredicateNode::make(exists_(limit_subselect), node_table_a);
  // clang-format on

  const auto actual_lqp = StrategyBaseTest::apply_rule(_rule, lqp);

  // If a limit node exists within the subselect of an exists expression, the lqp is not changed.
  EXPECT_LQP_EQ(actual_lqp, lqp);
}

}  // namespace opossum
