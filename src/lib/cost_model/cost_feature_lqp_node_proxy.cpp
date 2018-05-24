#include "cost_feature_lqp_node_proxy.hpp"

#include <cmath>

#include "logical_query_plan/abstract_lqp_node.hpp"
#include "logical_query_plan/join_node.hpp"
#include "logical_query_plan/predicate_node.hpp"
#include "logical_query_plan/sort_node.hpp"
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

    case CostFeature::LeftDataType:
    case CostFeature::RightDataType: {
      auto column_reference = LQPColumnReference{};

      if (_node->type() == LQPNodeType::Join) {
        const auto join_node = std::static_pointer_cast<JoinNode>(_node);
        const auto column_references = join_node->join_column_references();
        Assert(column_references, "No columns referenced in this JoinMode");

        column_reference =
            cost_feature == CostFeature::LeftDataType ? column_references->first : column_references->second;
      } else if (_node->type() == LQPNodeType::Predicate) {
        const auto predicate_node = std::static_pointer_cast<PredicateNode>(_node);
        if (cost_feature == CostFeature::LeftDataType) {
          column_reference = predicate_node->column_reference();
        } else {
          if (predicate_node->value().type() == typeid(AllTypeVariant)) {
            return data_type_from_all_type_variant(boost::get<AllTypeVariant>(predicate_node->value()));
          } else {
            Assert(predicate_node->value().type() == typeid(LQPColumnReference), "Expected LQPColumnReference");
            column_reference = boost::get<LQPColumnReference>(predicate_node->value());
          }
        }
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

      auto column_id = _node->get_output_column_id(column_reference);
      return _node->get_statistics()->column_statistics().at(column_id)->data_type();
    }

    case CostFeature::PredicateCondition:
      if (_node->type() == LQPNodeType::Join) {
        const auto predicate_condition = std::static_pointer_cast<JoinNode>(_node)->predicate_condition();
        Assert(predicate_condition, "No PredicateCondition in this JoinMode");
        return *predicate_condition;
      } else if (_node->type() == LQPNodeType::Predicate) {
        return std::static_pointer_cast<PredicateNode>(_node)->predicate_condition();
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

    case CostFeature::RightOperandIsColumn:
      if (_node->type() == LQPNodeType::Predicate) {
        return is_lqp_column_reference(std::static_pointer_cast<PredicateNode>(_node)->value());
      } else {
        Fail("CostFeature not defined for LQPNodeType");
      }

    default:
      break;
  }
  Fail("Feature extraction failed. Maybe the Feature should be handled in AbstractCostFeatureProxy?");
}

}  // namespace opossum
