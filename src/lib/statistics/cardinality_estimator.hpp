#pragma once

#include "abstract_cardinality_estimator.hpp"

namespace opossum {

class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
};

}  // namespace opossum
