#pragma once

#include "join_graph_statistics_cache.hpp"

namespace opossum {

class CardinalityEstimationCache {
 public:
  /**
   * Join/Predicate Ordering rules can enable this cache by calling `join_graph_statistics_cache.emplace()`. The docs of
   * JoinGraphStatisticsCache explain what this cache does.
   */
  std::optional<JoinGraphStatisticsCache> join_graph_statistics_cache;

  /**
   * Optimization Rules that !!!do not manipulate subplans after they have build them!!!! can enable
   * (by calling `*.emplace()) this cache. Statistics will the be cached with the pointer to the root node of the plan as the cache
   * key.
   */
  using PlanStatisticsCache =
      std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<TableStatistics>>;
  std::optional<PlanStatisticsCache> plan_statistics_cache;
};

}  // namespace opossum
