#pragma once

#include <memory>

#include "abstract_cost_estimator.hpp"

namespace hyrise {

class AbstractExpression;

/**
 * Cost model for logical complexity, i.e., approximate number of tuple accesses
 */
class CostEstimatorLogical : public AbstractCostEstimator {
 public:
  using AbstractCostEstimator::AbstractCostEstimator;

  std::shared_ptr<AbstractCostEstimator> new_instance() const override;

  Cost estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node, const bool cacheable = true) const override;
};

}  // namespace hyrise
