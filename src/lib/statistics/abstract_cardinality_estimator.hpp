#pragma once

#include <memory>

#include "cardinality.hpp"

namespace opossum {

class AbstractLQPNode;
class TableStatistics2;

class AbstractCardinalityEstimator {
 public:
  virtual ~AbstractCardinalityEstimator() = default;

  virtual Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;
  virtual std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const = 0;
};

}  // namespace opossum
