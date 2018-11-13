#pragma once

#include <memory>

#include "cardinality.hpp"

namespace opossum {

class AbstractLQPNode;
class OptimizationContext;
class TableStatistics2;

class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  virtual Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp,
                                           const std::shared_ptr<OptimizationContext>& context = {}) const = 0;
  virtual std::shared_ptr<TableStatistics2> estimate_statistics(
      const std::shared_ptr<AbstractLQPNode>& lqp, const std::shared_ptr<OptimizationContext>& context = {}) const = 0;
};

}  // namespace opossum
