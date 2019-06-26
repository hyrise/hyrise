#include "join_graph_statistics_cache.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

JoinGraphStatisticsCache JoinGraphStatisticsCache::from_join_graph(const JoinGraph& join_graph) {
  VertexIndexMap vertex_indices;
  for (auto vertex_idx = size_t{0}; vertex_idx < join_graph.vertices.size(); ++vertex_idx) {
    vertex_indices.emplace(join_graph.vertices[vertex_idx], vertex_idx);
  }

  PredicateIndexMap predicate_indices;

  auto predicate_idx = size_t{0};
  for (const auto& edge : join_graph.edges) {
    for (const auto& predicate : edge.predicates) {
      predicate_indices.emplace(predicate, predicate_idx++);
    }
  }

  return {std::move(vertex_indices), std::move(predicate_indices)};
}

JoinGraphStatisticsCache::JoinGraphStatisticsCache(VertexIndexMap&& vertex_indices,
                                                   PredicateIndexMap&& predicate_indices)
    : _vertex_indices(std::move(vertex_indices)), _predicate_indices(std::move(predicate_indices)) {}

std::optional<JoinGraphStatisticsCache::Bitmask> JoinGraphStatisticsCache::bitmask(
    const std::shared_ptr<AbstractLQPNode>& lqp) const {
  auto bitmask = std::optional<Bitmask>{_vertex_indices.size() + _predicate_indices.size()};

  visit_lqp(lqp, [&](const auto& node) {
    // Early out if `bitmask.reset()` was called during the search below
    if (!bitmask.has_value()) return LQPVisitation::DoNotVisitInputs;

    if (const auto vertex_iter = _vertex_indices.find(node); vertex_iter != _vertex_indices.end()) {
      DebugAssert(vertex_iter->second < bitmask->size(), "Vertex index out of range");
      bitmask->set(vertex_iter->second);
      return LQPVisitation::DoNotVisitInputs;

    } else if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_mode == JoinMode::Inner) {
        for (const auto& join_predicate : join_node->join_predicates()) {
          const auto predicate_index_iter = _predicate_indices.find(join_predicate);
          if (predicate_index_iter == _predicate_indices.end()) {
            bitmask.reset();
            return LQPVisitation::DoNotVisitInputs;
          } else {
            Assert(predicate_index_iter->second + _vertex_indices.size() < bitmask->size(),
                   "Predicate index out of range");
            bitmask->set(predicate_index_iter->second + _vertex_indices.size());
          }
        }
      } else if (join_node->join_mode == JoinMode::Cross) {
        return LQPVisitation::VisitInputs;
      } else {
        // Non-Inner/Cross join detected, cannot construct a bitmask from those
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      }

    } else if (const auto predicate_node = std::dynamic_pointer_cast<PredicateNode>(node)) {
      const auto predicate_index_iter = _predicate_indices.find(predicate_node->predicate());
      if (predicate_index_iter == _predicate_indices.end()) {
        bitmask.reset();
        return LQPVisitation::DoNotVisitInputs;
      } else {
        Assert(predicate_index_iter->second + _vertex_indices.size() < bitmask->size(), "Predicate index out of range");
        bitmask->set(predicate_index_iter->second + _vertex_indices.size());
      }

    } else if (node->type == LQPNodeType::Sort) {
      // ignore node type as it doesn't change the cardinality
    } else {
      bitmask.reset();
      return LQPVisitation::DoNotVisitInputs;
    }

    return LQPVisitation::VisitInputs;
  });

  return bitmask;
}

std::shared_ptr<TableStatistics> JoinGraphStatisticsCache::get(
    const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& requested_column_order) const {
  const auto cache_iter = _cache.find(bitmask);
  if (cache_iter == _cache.end()) {
    return nullptr;
  }

  /**
   * We found a matching cache_entry. Now, let's adjust its column order
   */

  const auto& cache_entry = cache_iter->second;

  const auto cached_table_statistics = cache_entry.table_statistics;

  // Compute the mapping from result column ids to cached column ids and the column data types
  // of the result
  auto cached_column_ids = std::vector<ColumnID>{requested_column_order.size()};
  auto result_column_data_types = std::vector<DataType>{requested_column_order.size()};
  for (auto column_id = ColumnID{0}; column_id < requested_column_order.size(); ++column_id) {
    const auto cached_column_id_iter = cache_entry.column_expression_order.find(requested_column_order[column_id]);
    Assert(cached_column_id_iter != cache_entry.column_expression_order.end(), "Column not found in cached statistics");
    const auto cached_column_id = cached_column_id_iter->second;
    result_column_data_types[column_id] = cached_table_statistics->column_data_type(cached_column_id);
    cached_column_ids[column_id] = cached_column_id;
  }

  // Allocate the TableStatistics to be returned
  auto output_column_statistics = std::vector<std::shared_ptr<BaseAttributeStatistics>>{requested_column_order.size()};

  // Bring AttributeStatistics into the requested order for each statistics slice
  for (auto column_id = ColumnID{0}; column_id < requested_column_order.size(); ++column_id) {
    const auto cached_column_id = cached_column_ids[column_id];
    const auto& cached_column_statistics = cached_table_statistics->column_statistics[cached_column_id];
    output_column_statistics[column_id] = cached_column_statistics;
  }

  const auto result_table_statistics =
      std::make_shared<TableStatistics>(std::move(output_column_statistics), cached_table_statistics->row_count);

  return result_table_statistics;
}

void JoinGraphStatisticsCache::set(const Bitmask& bitmask,
                                   const std::vector<std::shared_ptr<AbstractExpression>>& column_order,
                                   const std::shared_ptr<TableStatistics>& table_statistics) {
  auto cache_entry = CacheEntry{};
  cache_entry.table_statistics = table_statistics;

  for (auto column_id = ColumnID{0}; column_id < column_order.size(); ++column_id) {
    cache_entry.column_expression_order.emplace(column_order[column_id], column_id);
  }

  _cache.emplace(bitmask, std::move(cache_entry));
}
}  // namespace opossum
