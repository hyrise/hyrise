#pragma once

namespace opossum {

template<typename T>
Cardinality CardinalityEstimator::estimate_cardinality_of_inner_join_with_numeric_histograms(
  const std::shared_ptr<AbstractHistogram<T>>& histogram_left,
  const std::shared_ptr<AbstractHistogram<T>>& histogram_right
  ) {



}

}  // namespace opossum
