#include "greedy_operator_ordering.hpp"

#include <unordered_set>
#include <unordered_map>

#include "join_graph.hpp"
#include "optimizer/ca"

namespace opossum {

GreedyOperatorOrdering::GreedyOperatorOrdering(const std::shared_ptr<AbstractCostEstimator>& cost_estimator):
AbstractJoinOrderingAlgorithm(cost_estimator) {

}

std::shared_ptr<AbstractLQPNode> GreedyOperatorOrdering::operator()(const JoinGraph& join_graph) {
  auto vertex_clusters = std::unordered_map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};
  vertex_clusters.reserve(join_graph.vertices.size());
  for (auto vertex_idx = size_t{0}; vertex_idx <join_graph.vertices.size(); ++vertex_idx) {
    auto vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    vertex_set.set(vertex_idx);
    vertex_clusters.emplace(vertex_set, join_graph.vertices[vertex_idx]);
  }


  auto remaining_edges = std::unordered_set<size_t>{};
  remaining_edges.reserve(join_graph.edges.size());
  for (auto edge_idx = size_t{0}; edge_idx < join_graph.edges.size(); ++edge_idx) {
    remaining_edges.emplace(edge_idx);
  }

  auto edge_cardinalities = std::vector<std::pair<size_t, float>>{};
  edge_cardinalities.resize(join_graph.edges.size());
  for (auto edge_idx = size_t{0}; edge_idx < join_graph.edges.size(); ++edge_idx) {
    edge_cardinalities[edge_idx] = std::make_pair(edge_idx, _estimate_edge_cardinality(vertex_clusters, join_graph.edges[edge_idx]));
  }


  while (!remaining_edges.empty()) {

  }

}

}  // namespace opossum
