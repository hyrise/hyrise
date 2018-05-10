#include "abstract_cost_feature_proxy.hpp"

#include <cmath>

#include "logical_query_plan/abstract_lqp_node.hpp"

namespace opossum {

CostFeatureVariant AbstractCostFeatureProxy::extract_feature(const CostFeature cost_feature) const {
  switch (cost_feature) {
    case CostFeature::InputRowCountProduct:
      return {extract_feature(CostFeature::LeftInputRowCount).scalar() *
              extract_feature(CostFeature::RightInputRowCount).scalar()};
    case CostFeature::LeftInputReferenceRowCount:
      return extract_feature(CostFeature::LeftInputIsReferences).boolean()
                 ? extract_feature(CostFeature::LeftInputRowCount).scalar()
                 : 0.0f;
    case CostFeature::RightInputReferenceRowCount:
      return extract_feature(CostFeature::RightInputIsReferences).boolean()
                 ? extract_feature(CostFeature::RightInputRowCount).scalar()
                 : 0.0f;
    case CostFeature::LeftInputRowCountLogN: {
      const auto row_count = extract_feature(CostFeature::LeftInputRowCount).scalar();
      return row_count * std::log(row_count);
    }
    case CostFeature::RightInputRowCountLogN: {
      const auto row_count = extract_feature(CostFeature::RightInputRowCount).scalar();
      return row_count * std::log(row_count);
    }
    case CostFeature::LargerInputRowCount: {
      const auto left_input_row_count = extract_feature(CostFeature::LeftInputRowCount).scalar();
      const auto right_input_row_count = extract_feature(CostFeature::RightInputRowCount).scalar();
      return left_input_row_count > right_input_row_count ? left_input_row_count : right_input_row_count;
    }
    case CostFeature::SmallerInputRowCount: {
      const auto left_input_row_count = extract_feature(CostFeature::LeftInputRowCount).scalar();
      const auto right_input_row_count = extract_feature(CostFeature::RightInputRowCount).scalar();
      return left_input_row_count < right_input_row_count ? left_input_row_count : right_input_row_count;
    }
    case CostFeature::LargerInputReferenceRowCount: {
      return extract_feature(CostFeature::LeftInputIsMajor).boolean()
                 ? extract_feature(CostFeature::LeftInputReferenceRowCount).scalar()
                 : extract_feature(CostFeature::RightInputReferenceRowCount).scalar();
    }
    case CostFeature::SmallerInputReferenceRowCount: {
      return extract_feature(CostFeature::LeftInputIsMajor).boolean()
                 ? extract_feature(CostFeature::RightInputReferenceRowCount).scalar()
                 : extract_feature(CostFeature::LeftInputReferenceRowCount).scalar();
    }
    case CostFeature::OutputReferenceRowCount: {
      return extract_feature(CostFeature::LeftInputIsReferences).boolean()
                 ? extract_feature(CostFeature::OutputRowCount).scalar()
                 : 0.0f;
    }

    case CostFeature::LeftInputIsMajor: {
      const auto left_input_row_count = extract_feature(CostFeature::LeftInputRowCount).scalar();
      const auto right_input_row_count = extract_feature(CostFeature::RightInputRowCount).scalar();
      return left_input_row_count > right_input_row_count;
    }

    default:
      return _extract_feature_impl(cost_feature);
  }
}

}  // namespace opossum
