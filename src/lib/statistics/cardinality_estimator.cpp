#include "cardinality_estimator.hpp"

#include "table_statistics2.hpp"

namespace opossum {

Cardinality CardinalityEstimator::estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  return estimate_statistics(lqp)->row_count();
}

std::shared_ptr<TableStatistics2> CardinalityEstimator::estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const {

}

}  // namespace opossum
