#include <memory>

#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/null_scan_removal_rule.hpp"
#include "strategy_base_test.hpp"

using namespace hyrise::expression_functional;  // NOLINT

namespace hyrise {

class NullScanRemovalRuleTest : public StrategyBaseTest {
 public:
  void SetUp() override {
    rule = std::make_shared<NullScanRemovalRule>();
    mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    mock_node_column = mock_node->get_column("a");

    Hyrise::get().storage_manager.add_table("nullable_table",
                                            load_table("resources/test_data/tbl/int_float_null_1.tbl", ChunkOffset{2}));
    Hyrise::get().storage_manager.add_table("table",
                                            load_table("resources/test_data/tbl/int_float4_or_1.tbl", ChunkOffset{2}));
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
  // The rule can't apply on a node that is not of type Predicate.
  const auto expected_lqp = mock_node->deep_copy();
  const auto actual_lqp = apply_rule(rule, mock_node);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateIsNotNullExpression) {
  // The rule can't apply on a predicate that is not a null expression.
  const auto input_lqp = PredicateNode::make(equals_(mock_node_column, 42), mock_node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateConditionIsNotNull) {
  // The rule can't apply on a predicate which condition is not is not null.
  const auto input_lqp = PredicateNode::make(is_null_(mock_node_column), mock_node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateOperandIsNotLQPColumnExpression) {
  // The rule can't apply where the predicate operand is not a LQP Column expression.
  const auto input_lqp = PredicateNode::make(is_not_null_(42), mock_node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, LQPColumnOriginalNodeIsNotStoredTableNode) {
  // The rule can't apply where the original node of the LQP Column expression is not a storage table node.
  const auto input_lqp = PredicateNode::make(is_not_null_(mock_node_column), mock_node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNullable) {
  // The rule can't apply, if the column is not nullable
  const auto input_lqp = PredicateNode::make(is_not_null_(nullable_table_node_column), nullable_table_node);
  const auto expected_lqp = input_lqp->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNotNullable) {
  // All needed conditions to remove the node are matched:
  // 1. The node must be of type Predicate
  // 2. The predicate must be a null expression
  // 3. The predicate condition must be is not null
  // 4. The predicate operand needs to be an LQP Column expression
  // 5. The original node of the LQP Column expression needs to be a storage table node
  // 6. The column (referenced by the LQP Column expression) is not nullable
  const auto input_lqp = PredicateNode::make(is_not_null_(table_node_column), table_node);
  const auto expected_lqp = table_node->deep_copy();
  const auto actual_lqp = apply_rule(rule, input_lqp);

  EXPECT_LQP_EQ(actual_lqp, expected_lqp);
}

}  // namespace hyrise
