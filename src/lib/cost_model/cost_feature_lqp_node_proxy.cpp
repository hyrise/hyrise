#include "cost_feature_lqp_node_proxy.hpp"

#include <cmath>

#include "expression/between_expression.hpp"
#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
#include "operators/operator_join_predicate.hpp"
#include "operators/operator_scan_predicate.hpp"
#include "resolve_type.hpp"
#include "statistics/column_statistics.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

CostFeatureLQPNodeProxy::CostFeatureLQPNodeProxy(const std::shared_ptr<AbstractLQPNode>& node) : _node(node) {}

CostFeatureVariant CostFeatureLQPNodeProxy::_extract_feature_impl(const CostFeature cost_feature) const {
  switch (cost_feature) {
    case CostFeature::LeftInputRowCount:
      Assert(_node->left_input(), "Node doesn't have left input");
      return _node->left_input()->get_statistics()->row_count();
    case CostFeature::RightInputRowCount:
      Assert(_node->right_input(), "Node doesn't have left input");
      return _node->right_input()->get_statistics()->row_count();
    case CostFeature::LeftInputIsReferences:
      Assert(_node->left_input(), "Node doesn't have left input");
      return _node->left_input()->get_statistics()->table_type() == TableType::References;
    case CostFeature::RightInputIsReferences:
      Assert(_node->right_input(), "Node doesn't have left input");
      return _node->right_input()->get_statistics()->table_type() == TableType::References;
    case CostFeature::OutputRowCount:
      return _node->get_statistics()->row_count();

    default: {}
  }

  // For features that we can't extract from the interface of AbstractLQPNode, call a function dedicated to extract
  // features from this node type
  switch (_node->type) {
    case LQPNodeType::Predicate:
      return _extract_feature_from_predicate_node(cost_feature);
    case LQPNodeType::Join:
      return _extract_feature_from_join_node(cost_feature);
    default:
      Fail("CostFeature not defined for LQPNodeType");
  }
}

CostFeatureVariant CostFeatureLQPNodeProxy::_extract_feature_from_predicate_node(const CostFeature cost_feature) const {
  const auto predicate_node = std::static_pointer_cast<PredicateNode>(_node);

  // PredicateCondition::Between can only be extracted with this little hack, OperatorScanPredicate will split it up
  if (cost_feature == CostFeature::PredicateCondition &&
      std::dynamic_pointer_cast<BetweenExpression>(predicate_node->predicate)) {
    return PredicateCondition::Between;
  }
  const auto operator_predicates = OperatorScanPredicate::from_expression(
      *std::static_pointer_cast<PredicateNode>(_node)->predicate, *_node->left_input());
  Assert(operator_predicates, "Predicate too complex to extract a CostFeature from");
  const auto& operator_predicate = operator_predicates->at(0);

  switch (cost_feature) {
    case CostFeature::LeftDataType:
      return _node->column_expressions()[operator_predicate.column_id]->data_type();

    case CostFeature::RightDataType:
      if (operator_predicate.value.type() == typeid(AllTypeVariant)) {
        return data_type_from_all_type_variant(boost::get<AllTypeVariant>(operator_predicate.value));
      } else {
        Assert(is_column_id(operator_predicate.value), "Expected ColumnID");
        return _node->column_expressions()[boost::get<ColumnID>(operator_predicate.value)]->data_type();
      }

    case CostFeature::PredicateCondition:
      return operator_predicate.predicate_condition;

    case CostFeature::RightOperandIsColumn:
      return is_column_id(operator_predicate.value);

    default:
      Fail("Unexpected CostFeature");
  }
}

CostFeatureVariant CostFeatureLQPNodeProxy::_extract_feature_from_join_node(const CostFeature cost_feature) const {
  const auto operator_predicate = OperatorJoinPredicate::from_expression(
      *std::static_pointer_cast<JoinNode>(_node)->join_predicate, *_node->left_input(), *_node->right_input());
  Assert(operator_predicate, "Predicate too complex to extract a CostFeature from");

  switch (cost_feature) {
    case CostFeature::LeftDataType:
      return _node->left_input()->column_expressions()[operator_predicate->column_ids.first]->data_type();

    case CostFeature::RightDataType:
      return _node->right_input()->column_expressions()[operator_predicate->column_ids.second]->data_type();

    case CostFeature::PredicateCondition:
      return operator_predicate->predicate_condition;

    case CostFeature::RightOperandIsColumn:
      return true;

    default:
      Fail("Unexpected CostFeature");
  }
}

}  // namespace opossum
