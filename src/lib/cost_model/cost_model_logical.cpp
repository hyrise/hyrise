#include "cost_model_logical.hpp"

#include "abstract_cost_feature_proxy.hpp"
#include "operators/abstract_operator.hpp"

namespace opossum {

std::string CostModelLogical::name() const { return "CostModelLogical"; }

Cost CostModelLogical::get_reference_operator_cost(const std::shared_ptr<AbstractOperator>& op) const {
  return estimate_operator_cost(op);
}

Cost CostModelLogical::_cost_model_impl(const OperatorType operator_type,
                                        const AbstractCostFeatureProxy& feature_proxy) const {
  switch (operator_type) {
    case OperatorType::JoinHash:
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCount).scalar() +
             feature_proxy.extract_feature(CostFeature::RightInputRowCount).scalar();

    case OperatorType::TableScan:
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCount).scalar();

    case OperatorType::JoinSortMerge:
      // Model the cost of the sorting as the dominant cost
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCountLogN).scalar() +
             feature_proxy.extract_feature(CostFeature::RightInputRowCountLogN).scalar();

    case OperatorType::Product:
      return feature_proxy.extract_feature(CostFeature::InputRowCountProduct).scalar();

    case OperatorType::JoinNestedLoop:
      return feature_proxy.extract_feature(CostFeature::InputRowCountProduct).scalar();

    case OperatorType::UnionPositions:
      // Model the cost of the sorting as the dominant cost
      return feature_proxy.extract_feature(CostFeature::LeftInputRowCountLogN).scalar() +
             feature_proxy.extract_feature(CostFeature::RightInputRowCountLogN).scalar();

    default:
      return 0.0f;
  }
}

}  // namespace opossum
