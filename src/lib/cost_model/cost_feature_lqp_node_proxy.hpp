#pragma once

#include <memory>

#include "abstract_cost_feature_proxy.hpp"

namespace opossum {

class AbstractLQPNode;

class CostFeatureLQPNodeProxy : public AbstractCostFeatureProxy {
 public:
  explicit CostFeatureLQPNodeProxy(const std::shared_ptr<AbstractLQPNode>& node);

 protected:
  CostFeatureVariant _extract_feature_impl(const CostFeature cost_feature) const override;

 private:
  std::shared_ptr<AbstractLQPNode> _node;
};

}  // namespace opossum
