#pragma once

#include <iostream>
#include <memory>
#include <unordered_map>

#include "cost_model/cost.hpp"
#include "expression/abstract_expression.hpp"
#include "join_statistics_cache.hpp"

namespace opossum {

class AbstractLQPNode;
class TableStatistics2;

/**
 * Holds data shared by the Optimization Rules, the Cardinality Estimator and the Cost Estimator.
 */
class OptimizationContext {
 public:
  void print(std::ostream& stream = std::cout) const;

  void clear_caches();

  /**
   * Join/Predicate Ordering rules can enable this cache by calling `join_statistics_cache.emplace()`. The docs of
   * JoinStatisticsCache explain what this cache does.
   * This cache will be cleared and disabled after every optimization rule.
   */
  std::optional<JoinStatisticsCache> join_statistics_cache;

  /**
   * Optimization Rules that do not manipulate subplans after they have build them can enable (by calling `*.emplace())
   * these caches. Costs and Statistics will the be cached with the pointer to the root node of the plan as the cache
   * key.
   * These caches are cleared and disabled after every optimization rule.
   *
   * @{
   */
  using PlanStatisticsCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<TableStatistics2>>;
  std::optional<PlanStatisticsCache> plan_statistics_cache;

  using PlanCostCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;
  std::optional<PlanCostCache> plan_cost_cache;
  /**
   * @}
   */
};

};  // namespace opossum