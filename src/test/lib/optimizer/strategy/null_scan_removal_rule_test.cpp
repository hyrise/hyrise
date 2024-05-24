#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/null_scan_removal_rule.hpp"
#include "strategy_base_test.hpp"

namespace hyrise {

using namespace expression_functional;  // NOLINT(build/namespaces)

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
  std::shared_ptr<LQPColumnExpression> mock_node_column;
  std::shared_ptr<LQPColumnExpression> nullable_table_node_column;
  std::shared_ptr<LQPColumnExpression> table_node_column;
  std::shared_ptr<StoredTableNode> nullable_table_node;
  std::shared_ptr<StoredTableNode> table_node;
};

TEST_F(NullScanRemovalRuleTest, LQPNodeTypeIsNotPredicate) {
  // The rule can't apply on a node that is not of type Predicate.
  _lqp = mock_node->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, mock_node);
}

TEST_F(NullScanRemovalRuleTest, PredicateIsNotNullExpression) {
  // The rule cannot apply to a predicate that is not a null expression.
  _lqp = PredicateNode::make(equals_(mock_node_column, 42), mock_node);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateConditionIsNotNull) {
  // The rule can't apply on a predicate which condition is not is not null.
  _lqp = PredicateNode::make(is_null_(mock_node_column), mock_node);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, PredicateOperandIsNotLQPColumnExpression) {
  // The rule cannot apply if the predicate operand is not a LQP Column expression.
  _lqp = PredicateNode::make(is_not_null_(42), mock_node);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, LQPColumnOriginalNodeIsNotStoredTableNode) {
  // The rule can't apply if the original node of the LQP Column expression is not a storage table node.
  _lqp = PredicateNode::make(is_not_null_(mock_node_column), mock_node);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNullable) {
  // The rule cannot apply if the column is not nullable.
  _lqp = PredicateNode::make(is_not_null_(nullable_table_node_column), nullable_table_node);
  const auto expected_lqp = _lqp->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNotNullable) {
  // All required conditions to remove the node are matched:
  //   1. The node must be of type Predicate.
  //   2. The predicate must be a IsNullExpression.
  //   3. The predicate condition must be PredicateCondition::IsNotNull.
  //   4. The predicate operand needs to be an LQPColumnExpression.
  //   5. The original node of the LQPColumnExpression needs to be a StoredTableNode.
  //   6. The column (referenced by the LQPColumnExpression) is not nullable.
  _lqp = PredicateNode::make(is_not_null_(table_node_column), table_node);
  const auto expected_lqp = table_node->deep_copy();
  _apply_rule(rule, _lqp);

  EXPECT_LQP_EQ(_lqp, expected_lqp);
}

}  // namespace hyrise
