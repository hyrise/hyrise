#include "dp_ccp.hpp"

#include <cstddef>
#include <map>
#include <memory>
#include <utility>
#include <vector>

#include <boost/dynamic_bitset/dynamic_bitset.hpp>

#include "cost_estimation/abstract_cost_estimator.hpp"
#include "enumerate_ccp.hpp"
#include "join_graph.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"
#include "optimizer/join_ordering/join_graph_edge.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"

namespace hyrise {

std::shared_ptr<AbstractLQPNode> DpCcp::operator()(const JoinGraph& join_graph,
                                                   const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  Assert(!join_graph.vertices.empty(), "Code below relies on the JoinGraph having vertices");
  // No std::unordered_map because hashing of JoinGraphVertexSet is not (efficiently) possible: boost::dynamic_bitset
  // hides the data necessary for efficiently doing so.
  auto best_plan = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};

  /**
   * 1. Initialize best_plan[] with the vertices.
   */
  const auto vertex_count = join_graph.vertices.size();
  for (auto vertex_idx = size_t{0}; vertex_idx < vertex_count; ++vertex_idx) {
    auto single_vertex_set = JoinGraphVertexSet{vertex_count};
    single_vertex_set.set(vertex_idx);

    best_plan[single_vertex_set] = join_graph.vertices[vertex_idx];
  }

  /**
   * 2. Place Uncorrelated Predicates (think "6 > 4": not referencing any vertex).
   * 2.1 Collect uncorrelated predicates.
   */
  auto uncorrelated_predicates = std::vector<std::shared_ptr<AbstractExpression>>{};
  for (const auto& edge : join_graph.edges) {
    if (!edge.vertex_set.none()) {
      continue;
    }
    uncorrelated_predicates.insert(uncorrelated_predicates.end(), edge.predicates.begin(), edge.predicates.end());
  }

  /**
   * 2.2 Find the largest vertex and place the uncorrelated predicates for optimal execution.
   *     Reasoning: Uncorrelated predicates are either False or True for *all* rows. If an uncorrelated
   *                predicate is False and we place it on top of the largest vertex we avoid processing the vertex'
   *                many rows in later joins.
   */
  if (!uncorrelated_predicates.empty()) {
    // Find the largest vertex.
    auto largest_vertex_idx = size_t{0};
    auto largest_vertex_cardinality =
        cost_estimator->cardinality_estimator->estimate_cardinality(join_graph.vertices.front());

    for (auto vertex_idx = size_t{1}; vertex_idx < vertex_count; ++vertex_idx) {
      const auto vertex_cardinality =
          cost_estimator->cardinality_estimator->estimate_cardinality(join_graph.vertices[vertex_idx]);
      if (vertex_cardinality > largest_vertex_cardinality) {
        largest_vertex_idx = vertex_idx;
        largest_vertex_cardinality = vertex_cardinality;
      }
    }

    // Place the uncorrelated predicates on top of the largest vertex.
    auto largest_vertex_single_vertex_set = JoinGraphVertexSet{vertex_count};
    largest_vertex_single_vertex_set.set(largest_vertex_idx);
    auto largest_vertex_plan = best_plan[largest_vertex_single_vertex_set];
    for (const auto& uncorrelated_predicate : uncorrelated_predicates) {
      largest_vertex_plan = PredicateNode::make(uncorrelated_predicate, largest_vertex_plan);
    }
    best_plan[largest_vertex_single_vertex_set] = largest_vertex_plan;
  }

  /**
   * 3. Add local predicates on top of the vertices.
   */
  for (auto vertex_idx = size_t{0}; vertex_idx < vertex_count; ++vertex_idx) {
    const auto vertex_predicates = join_graph.find_local_predicates(vertex_idx);
    auto single_vertex_set = JoinGraphVertexSet{vertex_count};
    single_vertex_set.set(vertex_idx);

    auto& vertex_best_plan = best_plan[single_vertex_set];
    vertex_best_plan = _add_predicates_to_plan(vertex_best_plan, vertex_predicates, cost_estimator);
  }

  /**
   * 4. Prepare EnumerateCcp: Transform the JoinGraph's vertex-to-vertex edges into index pairs.
   */
  auto enumerate_ccp_edges = std::vector<std::pair<size_t, size_t>>{};
  for (const auto& edge : join_graph.edges) {
    // EnumerateCcp only deals with binary join predicates.
    if (edge.vertex_set.count() != 2) {
      continue;
    }

    const auto first_vertex_idx = edge.vertex_set.find_first();
    const auto second_vertex_idx = edge.vertex_set.find_next(first_vertex_idx);

    enumerate_ccp_edges.emplace_back(first_vertex_idx, second_vertex_idx);
  }

  /**
   * 5. Actual DpCcp algorithm: Enumerate the CsgCmpPairs; build candidate plans; update best_plan if the candidate plan
   *                            is cheaper than the cheapest currently known plan for a particular subset of vertices.
   */
  const auto csg_cmp_pairs = EnumerateCcp{vertex_count, enumerate_ccp_edges}();
  for (const auto& csg_cmp_pair : csg_cmp_pairs) {
    const auto best_plan_left_iter = best_plan.find(csg_cmp_pair.first);
    const auto best_plan_right_iter = best_plan.find(csg_cmp_pair.second);
    DebugAssert(best_plan_left_iter != best_plan.end() && best_plan_right_iter != best_plan.end(),
                "Subplan missing: either the JoinGraph is invalid or EnumerateCcp is buggy.");

    const auto join_predicates = join_graph.find_join_predicates(csg_cmp_pair.first, csg_cmp_pair.second);

    // Experiments for #2626 showed that this is the major bottleneck as there are three calls to the cost and
    // cardinality estimator.
    auto candidate_plan =
        _add_join_to_plan(best_plan_left_iter->second, best_plan_right_iter->second, join_predicates, cost_estimator);

    const auto joined_vertex_set = csg_cmp_pair.first | csg_cmp_pair.second;

    const auto best_plan_iter = best_plan.find(joined_vertex_set);
    // The following two calls to the cost estimator cause the second bottleneck.
    if (best_plan_iter == best_plan.end() || cost_estimator->estimate_plan_cost(candidate_plan) <
                                                 cost_estimator->estimate_plan_cost(best_plan_iter->second)) {
      best_plan.insert_or_assign(joined_vertex_set, candidate_plan);
    }
  }

  /**
   * 6. Build vertex set with all vertices and return the plan for it - this will be the best plan for the entire join
   *    graph.
   */
  auto all_vertices_set = JoinGraphVertexSet{vertex_count};
  all_vertices_set.flip();  // Turns all bits to '1'.

  const auto best_plan_iter = best_plan.find(all_vertices_set);
  Assert(best_plan_iter != best_plan.end(), "No plan for all vertices generated. Maybe JoinGraph is not connected?");

  return best_plan_iter->second;
}

}  // namespace hyrise
