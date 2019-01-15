#pragma once

#include <iostream>
#include <map>
#include <memory>
#include <unordered_map>

#include <boost/dynamic_bitset.hpp>
#include "cost_model/cost.hpp"
#include "expression/abstract_expression.hpp"

namespace opossum {

class AbstractLQPNode;
class AbstractExpression;
class TableStatistics2;

class OptimizationContext {
 public:
  void print(std::ostream& stream = std::cout) const;

  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> plan_leaf_indices;
  std::unordered_map<std::shared_ptr<AbstractExpression>, size_t> predicate_indices;

  struct TableStatisticsCacheEntry {
    std::shared_ptr<TableStatistics2> table_statistics;
    ExpressionUnorderedMap<ColumnID> column_expressions;
  };

  std::map<boost::dynamic_bitset<>, TableStatisticsCacheEntry> predicate_sets_cache;

  using PlanStatisticsCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, std::shared_ptr<TableStatistics2>>;
  std::optional<PlanStatisticsCache> plan_statistics_cache;

  using PlanCostCache = std::unordered_map<std::shared_ptr<AbstractLQPNode>, Cost>;
  std::optional<PlanCostCache> plan_cost_cache;
};

};  // namespace opossum