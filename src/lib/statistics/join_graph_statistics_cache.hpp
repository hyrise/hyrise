#pragma once

#include <map>
#include <unordered_map>

#include "boost/dynamic_bitset.hpp"

#include "expression/abstract_expression.hpp"

namespace opossum {

class TableStatistics;
class JoinGraph;

/**
 * Cache of TableStatistics for LQPs consisting exclusively of JoinNodes, PredicateNodes and
 * <Vertex Nodes>. That is, this is a cache for statistics associated with LQPs representing a subset of a JoinGraph.
 *
 * The key of the cache is a bitmask (with each bit representing a predicate or a vertex node).
 *
 * This cache exists primarily to aid the JoinOrderingRule. The JOR frequently request statistics for different plans
 * consisting of the same set of Join and Scan predicates.
 */
class JoinGraphStatisticsCache {
 public:
  using Bitmask = boost::dynamic_bitset<>;
  using VertexIndexMap = std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t>;
  using PredicateIndexMap = ExpressionUnorderedMap<size_t>;

  static JoinGraphStatisticsCache from_join_graph(const JoinGraph& join_graph);

  JoinGraphStatisticsCache(VertexIndexMap&& vertex_indices, PredicateIndexMap&& predicate_indices);

  /**
   * Try to build a bitmask (aka cache key) from an LQP. This will either return the bitmask or std::nullopt, if
   * Predicates or Vertices not registered in _vertex_indices and _predicate_indices are encountered.
   */
  std::optional<Bitmask> bitmask(const std::shared_ptr<AbstractLQPNode>& lqp) const;

  /**
   * Retrieve the cached statistics associated with @param bitmask. The order of columns in the returned TableStatistics
   * will be as specified by @param requested_column_order. Returns nullptr if no cache entry exists for the specified
   * bitmask.
   */
  std::shared_ptr<TableStatistics> get(
      const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& requested_column_order) const;

  /**
   * Put an entry [bitmask, table_statistics] into the cache.
   * @param column_order    Specifies the order of columns in @param table_statistics. This is required so
   *                        JoinGraphStatisticsCache::get() can return any requested column order
   */
  void set(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_order,
           const std::shared_ptr<TableStatistics>& table_statistics);

 private:
  const VertexIndexMap _vertex_indices;
  const PredicateIndexMap _predicate_indices;

  struct CacheEntry {
    std::shared_ptr<TableStatistics> table_statistics;
    // TableStatistics hold no info about which column corresponds to which expression. We need this info in
    // JoinGraphStatisticsCache::get(), so we cache it here.
    ExpressionUnorderedMap<ColumnID> column_expression_order;
  };

  // There is no std::hash<Bitmask>... :(
  std::map<Bitmask, CacheEntry> _cache;
};

}  // namespace opossum