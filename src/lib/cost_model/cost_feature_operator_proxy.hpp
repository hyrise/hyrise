#pragma once

#include <memory>

#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class AbstractOperator;

class CostFeatureOperatorProxy : public AbstractCostFeatureProxy {
 public:
  explicit CostFeatureOperatorProxy(const std::shared_ptr<AbstractOperator>& op);

 protected:
  CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const override;

 private:
  std::shared_ptr<AbstractOperator> _op;
};

}  // namespace opossum
