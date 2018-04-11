#pragma once

#include "cost_feature.hpp"

namespace opossum {

/**
 * Interface for wrappers around Operators and LQPNodes to extract CostFeatures independently of the wrapped type
 */
class AbstractCostFeatureProxy {
 public:
  virtual ~AbstractCostFeatureProxy() = default;

  CostFeatureVariant extract_feature(const CostFeature cost_feature) const;

 protected:
  /**
   * Implementation only needs to handle core features (CostFeature::LeftInputRowCount, etc).
   * Derived features (CostFeature::MajorInputRowCount, etc) will be computed in extract_feature()
   */
  virtual CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const = 0;
};

}  // namespace opossum
