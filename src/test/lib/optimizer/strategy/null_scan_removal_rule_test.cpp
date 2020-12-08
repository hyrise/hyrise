#include <memory>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/null_scan_removal_rule.hpp"
#include "strategy_base_test.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class NullScanRemovalRuleTest : public StrategyBaseTest {};

TEST_F(NullScanRemovalRuleTest, LQPNodeTypeIsNotPredicate) {
  auto input_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  auto rule = std::make_shared<NullScanRemovalRule>();

  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateIsNotNullExpression) {
  auto node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto a_a = node_a->get_column("a");

  const auto input_lqp = PredicateNode::make(equals_(a_a, 42), node_a);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateConditionIsNotNull) {
  auto node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto a_a = node_a->get_column("a");

  const auto input_lqp = PredicateNode::make(is_null_(a_a), node_a);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateOperandIsNotLQPColumnExpression) {
  auto node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto a_a = node_a->get_column("a");

  const auto input_lqp = PredicateNode::make(is_not_null_(42), node_a);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, LQPColumnOriginalNodeIsNotStoredTableNode) {
  // Test that column original node is not a stored table node.
  auto node_a = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto a_a = node_a->get_column("a");

  const auto input_lqp = PredicateNode::make(is_not_null_(a_a), node_a);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNullable) {
  // table column is nullable, so no node should be removed.
  Hyrise::get().storage_manager.add_table("table_a", load_table("resources/test_data/tbl/int_float_null_1.tbl", 2));
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto _table_node = StoredTableNode::make("table_a");
  auto column = lqp_column_(_table_node, ColumnID{0});

  const auto input_lqp = PredicateNode::make(is_not_null_(column), _table_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNotNullable) {
  Hyrise::get().storage_manager.add_table("table_a", load_table("resources/test_data/tbl/int_float4_or_1.tbl", 2));
  auto rule = std::make_shared<NullScanRemovalRule>();
  auto _table_node = StoredTableNode::make("table_a");
  auto column = lqp_column_(_table_node, ColumnID{0});

  const auto input_lqp = PredicateNode::make(is_not_null_(column), _table_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = _table_node->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
