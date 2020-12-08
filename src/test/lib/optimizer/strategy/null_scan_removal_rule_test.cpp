#include <memory>

#include "base_test.hpp"
#include "expression/expression_functional.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/stored_table_node.hpp"
#include "optimizer/strategy/null_scan_removal_rule.hpp"

using namespace opossum::expression_functional;  // NOLINT

namespace opossum {

class NullScanRemovalRuleTest : public BaseTest {};

TEST_F(NullScanRemovalRuleTest, LQPNodeTypeIsNotPredicate) {
    mock_node = MockNode::make(MockNode::ColumnDefinitions{{DataType::Int, "a"}, {DataType::Float, "b"}});
    

}

TEST_F(NullScanRemovalRuleTest, PredicateIsNotNullExpression) {


}

TEST_F(NullScanRemovalRuleTest, PredicateConditionIsNotNull) {


}

TEST_F(NullScanRemovalRuleTest, PredicateOperandIsNotLQPColumnExpression) {


}

TEST_F(NullScanRemovalRuleTest, LQPColumnOriginalNodeIsNotStoredTableNode) {


}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNullable) {


}

TEST_F(NullScanRemovalRuleTest, TableColumnDefinitionIsNotNullable) {


}
























}  // namespace opossum