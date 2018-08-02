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

  CostFeatureVariant _extract_feature_from_predicate_node(const CostFeature cost_feature) const;
  CostFeatureVariant _extract_feature_from_join_node(const CostFeature cost_feature) const;
};

}  // namespace opossum
