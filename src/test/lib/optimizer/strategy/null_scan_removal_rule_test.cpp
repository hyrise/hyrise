#include <memory>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/null_scan_removal_rule.hpp"
#include "strategy_base_test.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class NullScanRemovalRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    rule = std::make_shared<NullScanRemovalRule>();
    mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    mock_node_column = mock_node->get_column("a");

    Hyrise::get().storage_manager.add_table("nullable_table",
                                            load_table("resources/test_data/tbl/int_float_null_1.tbl", 2));
    Hyrise::get().storage_manager.add_table("table", load_table("resources/test_data/tbl/int_float4_or_1.tbl", 2));
    nullable_table_node = StoredTableNode::make("nullable_table");
    table_node = StoredTableNode::make("table");
    nullable_table_node_column = lqp_column_(nullable_table_node, ColumnID{0});
    table_node_column = lqp_column_(table_node, ColumnID{0});
  }
  std::shared_ptr<MockNode> mock_node;
  std::shared_ptr<NullScanRemovalRule> rule;
  std::shared_ptr<LQPColumnExpression> mock_node_column, nullable_table_node_column, table_node_column;
  std::shared_ptr<StoredTableNode> nullable_table_node, table_node;
};

TEST_F(NullScanRemovalRuleTest, LQPNodeTypeIsNotPredicate) {
  const auto actual_lqp = apply_rule(rule, mock_node);
  const auto expected_lqp = mock_node->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateIsNotNullExpression) {
  const auto input_lqp = PredicateNode::make(equals_(mock_node_column, 42), mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateConditionIsNotNull) {
  const auto input_lqp = PredicateNode::make(is_null_(mock_node_column), mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateOperandIsNotLQPColumnExpression) {
  const auto input_lqp = PredicateNode::make(is_not_null_(42), mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, LQPColumnOriginalNodeIsNotStoredTableNode) {
  // Test that column original node is not a stored table node.
  const auto input_lqp = PredicateNode::make(is_not_null_(mock_node_column), mock_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNullable) {
  // table column is nullable, so no node should be removed.
  const auto input_lqp = PredicateNode::make(is_not_null_(nullable_table_node_column), nullable_table_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = input_lqp->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNotNullable) {
  const auto input_lqp = PredicateNode::make(is_not_null_(table_node_column), table_node);
  const auto actual_lqp = apply_rule(rule, input_lqp);
  const auto expected_lqp = table_node->deep_copy();

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace opossum
