#include "greedy_operator_ordering.hpp"

#include <numeric>
#include <unordered_map>
#include <unordered_set>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "join_graph.hpp"
#include "statistics/abstract_cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<AbstractLQPNode> GreedyOperatorOrdering::operator()(
    const JoinGraph& join_graph, const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  DebugAssert(!join_graph.vertices.empty(), "Code below relies on there being at least one vertex");

  /**
   * 1. Initialize
   *
   * 1.1 Initialize the "vertex clusters".
   *       Initially, each vertex cluster consists of a single vertex and is associated with a plan that is just the
   *       vertex. During the algorithm, vertex clusters are consecutively joined.
   */
  auto vertex_clusters = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};
  for (auto vertex_idx = size_t{0}; vertex_idx < join_graph.vertices.size(); ++vertex_idx) {
    auto vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    vertex_set.set(vertex_idx);
    vertex_clusters.emplace(vertex_set, join_graph.vertices[vertex_idx]);
  }

  /**
   * 1.2 Initialize...
   *   - "uncorrelated_predicates", which are all predicates that do not reference any of the vertices. At the end,
   *     we will just place them on top of the plan
   *   - "remaining_edge_indices", which initially contains the indices of all edges (except for uncorrelated edges).
   *     Each step of the algorithm we select one edge; join the vertex clusters that it connects and remove it from the
   *     set again.
   */
  auto uncorrelated_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  auto remaining_edge_indices = std::vector<size_t>{};
  for (auto edge_idx = size_t{0}; edge_idx < join_graph.edges.size(); ++edge_idx) {
    const auto& edge = join_graph.edges[edge_idx];

    if (edge.vertex_set.none()) {
      uncorrelated_predicates.insert(uncorrelated_predicates.end(), edge.predicates.begin(), edge.predicates.end());
    } else {
      remaining_edge_indices.emplace_back(edge_idx);
    }
  }

  /**
   * 1.3 Initialize "plan_by_edge". At all times, for each (uncorrelated) edge, we hold a plan at hand that would be
   *       produced when joining the vertex clusters that this edge connects.
   *       We need this plan both to
   *         -> determine the edge with the lowest cardinality
   *         -> set the plan associated with the newly formed vertex cluster, when joining the clusters with the lowest
   *          cardinality
   *
   */
  auto plan_by_edge = std::vector<PlanCardinalityPair>{join_graph.edges.size()};
  for (const auto& edge_idx : remaining_edge_indices) {
    plan_by_edge[edge_idx] = _build_plan_for_edge(join_graph.edges[edge_idx], vertex_clusters, cost_estimator);
  }

  /**
   * 2. Main loop of the algorithm.
   *      During each iteration, we select the edge that has a minimum cardinality and join the vertex clusters that it
   *      connects
   */

  while (!remaining_edge_indices.empty()) {
    /**
     * 2.1 Find the edge with the lowest cardinality and remove it from "remaining_edge_indices"
     */
    std::sort(remaining_edge_indices.begin(), remaining_edge_indices.end(),
              [&](const auto& lhs, const auto& rhs) { return plan_by_edge[lhs].second > plan_by_edge[rhs].second; });

    const auto lowest_cardinality_edge_idx = remaining_edge_indices.back();
    remaining_edge_indices.pop_back();

    const auto& lowest_cardinality_edge = join_graph.edges[lowest_cardinality_edge_idx];

    /**
     * 2.2 Determine the vertex set "joined_vertex_set" of the new cluster created by joining all clusters connected by
     *      the edge with the lowest cardinality.
     *      While doing so, remove all the individual vertex clusters connected by the edge from the set of
     *      vertex_clusters - we will add a new vertex cluster comprised of all of them afterwards
     */
    auto joined_vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    for (auto vertex_cluster_iter = vertex_clusters.begin(); vertex_cluster_iter != vertex_clusters.end();) {
      if ((vertex_cluster_iter->first & lowest_cardinality_edge.vertex_set).any()) {
        joined_vertex_set |= vertex_cluster_iter->first;
        vertex_cluster_iter = vertex_clusters.erase(vertex_cluster_iter);
      } else {
        ++vertex_cluster_iter;
      }
    }

    /**
     * 2.3 Add a new vertex cluster consisting of all the vertex clusters joined by the edge with the lowest cardinality
     */
    vertex_clusters.emplace(joined_vertex_set, plan_by_edge[lowest_cardinality_edge_idx].first);

    /**
     * 2.4 Update the plans associated with all edges that connect to the newly created cluster.
     */
    for (const auto& remaining_edge_idx : remaining_edge_indices) {
      const auto& remaining_edge = join_graph.edges[remaining_edge_idx];
      if ((remaining_edge.vertex_set & joined_vertex_set).any()) {
        plan_by_edge[remaining_edge_idx] = _build_plan_for_edge(remaining_edge, vertex_clusters, cost_estimator);
      }
    }
  }

  /**
   * 3. Assert that all clusters where joined and, before returning, add the uncorrelated predicates on top of the plan
   */
  Assert(vertex_clusters.size() == 1 && vertex_clusters.begin()->first.all(),
         "No cluster for all vertices generated, is the JoinGraph connected?");
  const auto result_plan = vertex_clusters.begin()->second;
  return _add_predicates_to_plan(result_plan, uncorrelated_predicates, cost_estimator);
}

GreedyOperatorOrdering::PlanCardinalityPair GreedyOperatorOrdering::_build_plan_for_edge(
    const JoinGraphEdge& edge, const std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>& vertex_clusters,
    const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  auto joined_clusters = std::vector<JoinGraphVertexSet>{};

  for (auto& vertex_cluster : vertex_clusters) {
    if ((vertex_cluster.first & edge.vertex_set).any()) {
      joined_clusters.emplace_back(vertex_cluster.first);
    }
  }

  DebugAssert(!joined_clusters.empty(),
              "Edge appearing passed to this function should reference at least one vertex cluster");

  const auto& cardinality_estimator = cost_estimator->cardinality_estimator;

  if (joined_clusters.size() == 2) {
    // Binary join edges
    const auto plan = _add_join_to_plan(vertex_clusters.at(joined_clusters[0]), vertex_clusters.at(joined_clusters[1]),
                                        edge.predicates, cost_estimator);

    return {plan, cardinality_estimator->estimate_cardinality(plan)};

  } else {
    // Local edges and hyperedges
    auto plan = vertex_clusters.at(joined_clusters.front());

    for (auto joined_cluster_idx = size_t{1}; joined_cluster_idx < joined_clusters.size(); ++joined_cluster_idx) {
      plan = JoinNode::make(JoinMode::Cross, plan, vertex_clusters.at(joined_clusters[joined_cluster_idx]));
    }

    plan = _add_predicates_to_plan(plan, edge.predicates, cost_estimator);
    return {plan, cardinality_estimator->estimate_cardinality(plan)};
  }
}

}  // namespace opossum
