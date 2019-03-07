#include "abstract_cardinality_estimator.hpp"

namespace opossum {

void AbstractCardinalityEstimator::guarantee_join_graph(const JoinGraph& join_graph) {
  cardinality_estimation_cache.join_graph_statistics_cache.emplace(
      JoinGraphStatisticsCache::from_join_graph(join_graph));
}

void AbstractCardinalityEstimator::guarantee_bottom_up_construction() {
  cardinality_estimation_cache.statistics_by_lqp.emplace();
}

}  // namespace opossum
