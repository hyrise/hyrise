#include "join_statistics_cache.hpp"

#include "logical_query_plan/lqp_utils.hpp"
#include "optimizer/join_ordering/join_graph.hpp"
#include "statistics/table_statistics2.hpp"
#include "statistics/chunk_statistics2.hpp"

namespace {

using namespace opossum; // NOLINT


}  // namespace

namespace opossum {

JoinStatisticsCache JoinStatisticsCache::from_join_graph(const JoinGraph& join_graph) {
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

JoinStatisticsCache::JoinStatisticsCache(VertexIndexMap&& vertex_indices, PredicateIndexMap&& predicate_indices):
_vertex_indices(std::move(vertex_indices)), _predicate_indices(std::move(predicate_indices)) {}

std::optional<JoinStatisticsCache::Bitmask> JoinStatisticsCache::bitmask(const std::shared_ptr<AbstractLQPNode>& lqp) const {
  auto bitmask = std::optional<Bitmask>{_vertex_indices.size() + _predicate_indices.size()};
  
  visit_lqp(lqp, [&](const auto& node) {
    if (!bitmask) return LQPVisitation::DoNotVisitInputs;

    if (const auto vertex_iter = _vertex_indices.find(node); vertex_iter != _vertex_indices.end()) {
      DebugAssert(vertex_iter->second < bitmask->size(), "Vertex index out of range");
      bitmask->set(vertex_iter->second);
      return LQPVisitation::DoNotVisitInputs;

    } else if (const auto join_node = std::dynamic_pointer_cast<JoinNode>(node)) {
      if (join_node->join_mode == JoinMode::Inner) {
        const auto predicate_index_iter = _predicate_indices.find(join_node->join_predicate());
        if (predicate_index_iter == _predicate_indices.end()) {
          bitmask.reset();
          return LQPVisitation::DoNotVisitInputs;
        } else {
          Assert(predicate_index_iter->second + _vertex_indices.size() < bitmask->size(),
                 "Predicate index out of range");
          bitmask->set(predicate_index_iter->second + _vertex_indices.size());
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

    } else if (node->type == LQPNodeType::Validate || node->type == LQPNodeType::Sort) {
      // ignore node type as it doesn't change the cardinality
    } else {
      bitmask.reset();
      return LQPVisitation::DoNotVisitInputs;
    }

    return LQPVisitation::VisitInputs;
  });
  
  return bitmask;
}

std::shared_ptr<TableStatistics2> JoinStatisticsCache::get(const Bitmask& bitmask,
const std::vector<std::shared_ptr<AbstractExpression>>& requested_column_order) const {
  const auto cache_iter = _cache.find(bitmask);
  if (cache_iter == _cache.end()) {
    return nullptr;
  }

  /**
   * We found a matching cache_entry. Now, let's adjust its column order
   */

  const auto &cache_entry = cache_iter->second;

  DebugAssert(requested_column_order.size() == cache_entry.column_expression_order.size(),
              "Wrong number in requested column order");

  const auto cached_table_statistics = cache_entry.table_statistics;

  // Allocate the TableStatistics, ChunkStatisticsSet and ChunkStatistics to be returned
  const auto result_table_statistics = std::make_shared<TableStatistics2>();
  result_table_statistics->chunk_statistics_sets.reserve(cached_table_statistics->chunk_statistics_sets.size());

  for (const auto &cached_chunk_statistics_set : cached_table_statistics->chunk_statistics_sets) {
    auto result_chunk_statistics_set = ChunkStatistics2Set{};
    result_chunk_statistics_set.reserve(cached_chunk_statistics_set.size());

    for (const auto &cached_chunk_statistics : cached_chunk_statistics_set) {
      const auto result_chunk_statistics = std::make_shared<ChunkStatistics2>(cached_chunk_statistics->row_count);
      result_chunk_statistics->segment_statistics.resize(cached_chunk_statistics->segment_statistics.size());
      result_chunk_statistics->approx_invalid_row_count = cached_chunk_statistics->approx_invalid_row_count;
      result_chunk_statistics_set.emplace_back(result_chunk_statistics);
    }

    result_table_statistics->chunk_statistics_sets.emplace_back(result_chunk_statistics_set);
  }

  // For each column in the requested column order, lookup the column id in the CacheEntry and clone all
  // SegmentStatistics from it
  for (auto column_id = ColumnID{0}; column_id < requested_column_order.size(); ++column_id) {
    const auto cached_column_id_iter = cache_entry.column_expression_order.find(requested_column_order[column_id]);
    Assert(cached_column_id_iter != cache_entry.column_expression_order.end(),
           "Column not found in cached statistics");
    const auto cached_column_id = cached_column_id_iter->second;

    for (auto chunk_statistics_set_idx = size_t{0};
         chunk_statistics_set_idx < cached_table_statistics->chunk_statistics_sets.size();
         ++chunk_statistics_set_idx) {
      const auto &cached_chunk_statistics_set =
      cached_table_statistics->chunk_statistics_sets[chunk_statistics_set_idx];

      for (auto chunk_id = ChunkID{0}; chunk_id < cached_chunk_statistics_set.size(); ++chunk_id) {
        const auto &cached_chunk_statistics =
        cached_table_statistics->chunk_statistics_sets[chunk_statistics_set_idx][chunk_id];
        const auto &chunk_statistics =
        result_table_statistics->chunk_statistics_sets[chunk_statistics_set_idx][chunk_id];
        chunk_statistics->segment_statistics[column_id] =
        cached_chunk_statistics->segment_statistics[cached_column_id];
      }
    }
  }

  return result_table_statistics;
}

void JoinStatisticsCache::set(const Bitmask& bitmask, const std::vector<std::shared_ptr<AbstractExpression>>& column_order, const std::shared_ptr<TableStatistics2>& table_statistics) {
  auto cache_entry = CacheEntry{};
  cache_entry.table_statistics = table_statistics;

  for (auto column_id = ColumnID{0}; column_id < column_order.size(); ++column_id) {
    cache_entry.column_expression_order.emplace(column_order[column_id], column_id);
  }

  _cache.emplace(bitmask, std::move(cache_entry));
}
}  // namespace opossum
