#pragma once

#include <memory>

#include "abstract_cardinality_estimator.hpp"

namespace opossum {

template <typename T>
class AbstractHistogram;

class CardinalityEstimator : public AbstractCardinalityEstimator {
 public:
  Cardinality estimate_cardinality(const std::shared_ptr<AbstractLQPNode>& lqp) const override;
  std::shared_ptr<TableStatistics2> estimate_statistics(const std::shared_ptr<AbstractLQPNode>& lqp) const override;

  template <typename T>
  static Cardinality estimate_cardinality_of_inner_equi_join_with_numeric_histograms(
      const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
      const std::shared_ptr<AbstractHistogram<T>>& histogram_right);
};

}  // namespace opossum

#include "cardinality_estimator.ipp"
