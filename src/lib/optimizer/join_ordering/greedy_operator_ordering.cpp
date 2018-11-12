#include "greedy_operator_ordering.hpp"

#include <numeric>
#include <unordered_set>
#include <unordered_map>

#include "join_graph.hpp"
#include "statistics/table_statistics.hpp"

namespace opossum {

GreedyOperatorOrdering::GreedyOperatorOrdering(const std::shared_ptr<AbstractCostEstimator>& cost_estimator):
AbstractJoinOrderingAlgorithm(cost_estimator) {

}

std::shared_ptr<AbstractLQPNode> GreedyOperatorOrdering::operator()(const JoinGraph& join_graph) {
  auto vertex_clusters = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};
  for (auto vertex_idx = size_t{0}; vertex_idx <join_graph.vertices.size(); ++vertex_idx) {
    auto vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    vertex_set.set(vertex_idx);
    vertex_clusters.emplace(vertex_set, join_graph.vertices[vertex_idx]);
  }


  auto remaining_edge_indices = std::vector<size_t>{join_graph.edges.size()};
  std::iota(remaining_edge_indices.begin(), remaining_edge_indices.end(), 0);

  auto plan_by_edge = std::vector<std::shared_ptr<AbstractLQPNode>>{join_graph.edges.size()};
  for (auto edge_idx = size_t{0}; edge_idx < join_graph.edges.size(); ++edge_idx) {
    plan_by_edge[edge_idx] = _build_plan_for_edge(vertex_clusters, join_graph.edges[edge_idx]);
  }

  while (!remaining_edge_indices.empty()) {
    std::sort(remaining_edge_indices.begin(), remaining_edge_indices.end(), [&](const auto& lhs, const auto& rhs) {
      return plan_by_edge[lhs]->get_statistics()->row_count() > plan_by_edge[rhs]->get_statistics()->row_count();
    });

    const auto edge_idx = remaining_edge_indices.back();
    remaining_edge_indices.pop_back();

    const auto& edge = join_graph.edges[edge_idx];

    auto new_vertex_cluster = JoinGraphVertexSet{join_graph.edges.size()};
    auto edge_indices_with_changed_plans = std::unordered_set<size_t>{};

    for (auto vertex_cluster_iter = vertex_clusters.begin(); vertex_cluster_iter != vertex_clusters.end(); ) {
      if ((vertex_cluster_iter->first & edge.vertex_set).any()) {
        vertex_cluster_iter = vertex_clusters.erase(vertex_cluster_iter);
      } else {
        ++vertex_cluster_iter;
      }
      new_vertex_cluster |= vertex_cluster_iter->first;
    }

    vertex_clusters.emplace(new_vertex_cluster, plan_by_edge[edge_idx]);

    for (const auto& edge_idx2 : remaining_edge_indices) {
      const auto& edge2 = join_graph.edges[edge_idx];
      if ((edge2.vertex_set & new_vertex_cluster).any()) {
        plan_by_edge[edge_idx2] =  _build_plan_for_edge(vertex_clusters, join_graph.edges[edge_idx]);
      }
    }

  }

  auto all_vertices = JoinGraphVertexSet{join_graph.vertices.size()};
  all_vertices.flip();

  const auto vertex_cluster_iter = vertex_clusters.find(all_vertices);
  Assert(vertex_cluster_iter != vertex_clusters.end(), "No cluster for all vertices generated, is the JoinGraph connected?");

  return vertex_cluster_iter->second;
}

std::shared_ptr<AbstractLQPNode> GreedyOperatorOrdering::_build_plan_for_edge(const std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>& vertex_clusters,
const JoinGraphEdge& edge) const {

  auto joined_clusters = std::vector<JoinGraphVertexSet>{};

  for (auto& vertex_cluster : vertex_clusters) {
    if ((vertex_cluster.first & edge.vertex_set).any()) {
      joined_clusters.emplace_back(vertex_cluster.first);
    }
  }

  DebugAssert(joined_clusters.size() != 0, "Each edge should point to at least one cluster.");

  if (joined_clusters.size() == 2) {
    return _add_join_to_plan(
      vertex_clusters.at(joined_clusters[0]),
      vertex_clusters.at(joined_clusters[1]),
      edge.predicates
    );
  } else {
    auto plan = vertex_clusters.at(joined_clusters.front());

    for (auto joined_cluster_idx = size_t{1}; joined_cluster_idx < joined_clusters.size(); ++joined_cluster_idx) {
      plan = JoinNode::make(JoinMode::Cross, plan, vertex_clusters.at(joined_clusters[joined_cluster_idx]));
    }

    return _add_predicates_to_plan(plan, edge.predicates);
  }

}

}  // namespace opossum
