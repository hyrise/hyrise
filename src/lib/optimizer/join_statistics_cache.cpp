#include "join_statistics_cache.hpp"

namespace opossum {

std::optional<JoinStatisticsCache::Bitmask> JoinStatisticsCache::bitmask(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  auto bitmask = std::optional<Bitmask>{plan_leaf_indices.size() + predicate_indices.size()};
  populate_cache_bitmask(lqp, context, bitmask);
  return bitmask;
}

std::shared_ptr<TableStatistics2> JoinStatisticsCache::get(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions) const {

}

void JoinStatisticsCache::set(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_expressions, const std::shared_ptr<TableStatistics2>& table_statistics) const {

}

}  // namespace opossum
