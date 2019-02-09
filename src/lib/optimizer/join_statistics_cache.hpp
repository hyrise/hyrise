#pragma once

#include <map>
#include <unordered_map>

#include "boost/dynamic_bitset.hpp"

#include "expression/abstract_expression.hpp"

namespace opossum {

class TableStatistics2;

/**
 * Cache of TableStatistics for LQPs consisting exclusively of JoinNodes, PredicateNodes and <Leaf Nodes>.
 * This cache exists primarily to aid the JoinOrderingRule. The JOR frequently request statistics for different plans
 * consisting of the same set of Join and Scan predicates.
 */
class JoinStatisticsCache {
 public:
  using Bitmask = boost::dynamic_bitset<>;

  std::optional<Bitmask> bitmask(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  std::shared_ptr<TableStatistics2> get(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions) const;
  void set(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions, const std::shared_ptr<TableStatistics2>& table_statistics) const;

 private:
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> plan_leaf_indices;
  std::unordered_map<std::shared_ptr<AbstractExpression>, size_t> predicate_indices;

  struct TableStatisticsCacheEntry {
    std::shared_ptr<TableStatistics2> table_statistics;
    ExpressionUnorderedMap<ColumnID> column_expressions;
  };

  std::map<boost::dynamic_bitset<>, TableStatisticsCacheEntry> predicate_sets_cache;
};


}