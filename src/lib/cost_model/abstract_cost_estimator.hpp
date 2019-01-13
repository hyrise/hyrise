#pragma once

#include <memory> // NEEDEDINCLUDE

#include "cost.hpp" // NEEDEDINCLUDE

namespace opossum {

class AbstractLQPNode;

/**
 * Interface of an algorithm that predicts Cost for operators.
 */
class AbstractCostEstimator {
 public:
  virtual ~AbstractCostEstimator() = default;

  Cost estimate_plan_cost(const std::shared_ptr<AbstractLQPNode>& lqp) const;

 protected:
  virtual Cost _estimate_node_cost(const std::shared_ptr<AbstractLQPNode>& node) const = 0;
};

}  // namespace opossum
