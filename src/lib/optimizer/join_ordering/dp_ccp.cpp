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
#include "statistics/abstract_cardinality_estimator.hpp"
#include "statistics/cardinality_estimator.hpp"
#include "utils/assert.hpp"
#include "utils/timer.hpp"
#include "utils/format_duration.hpp"

namespace hyrise {

std::shared_ptr<AbstractLQPNode> DpCcp::operator()(const JoinGraph& join_graph,
                                                   const std::shared_ptr<AbstractCostEstimator>& cost_estimator) {
  Assert(!join_graph.vertices.empty(), "Code below relies on the JoinGraph having vertices");
  auto overall_timer = Timer{};

  // No std::unordered_map as hashing of JoinGraphVertexSet is not (efficiently) possible because
  // boost::dynamic_bitset hides the data necessary for doing so efficiently.
  auto best_plan = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};

  auto timer = Timer{};

  /**
   * 1. Initialize best_plan[] with the vertices.
   */
  const auto vertex_count = join_graph.vertices.size();
  for (auto vertex_idx = size_t{0}; vertex_idx < vertex_count; ++vertex_idx) {
    auto single_vertex_set = JoinGraphVertexSet{vertex_count};
    single_vertex_set.set(vertex_idx);

    best_plan[single_vertex_set] = join_graph.vertices[vertex_idx];
  }

  const auto duration1 = timer.lap();

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

  const auto  duration21 = timer.lap();

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
  const auto  duration22 = timer.lap();

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
  const auto  duration3 = timer.lap();

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
  const auto  duration4 = timer.lap();

  /**
   * 5. Actual DpCcp algorithm: Enumerate the CsgCmpPairs; build candidate plans; update best_plan if the candidate plan
   *                            is cheaper than the cheapest currently known plan for a particular subset of vertices.
   */
  const auto csg_cmp_pairs = EnumerateCcp{vertex_count, enumerate_ccp_edges}();

  const auto duration51 = timer.lap();
  auto duration52 = std::chrono::nanoseconds{};
  auto duration53 = std::chrono::nanoseconds{};
  auto duration54 = std::chrono::nanoseconds{};
  auto duration55 = std::chrono::nanoseconds{};
  auto duration56 = std::chrono::nanoseconds{};
  auto duration57 = std::chrono::nanoseconds{};
  auto duration571 = std::chrono::nanoseconds{};
  auto duration572 = std::chrono::nanoseconds{};
  auto duration58 = std::chrono::nanoseconds{};

  for (const auto& csg_cmp_pair : csg_cmp_pairs) {
    auto pair_timer = Timer{};
    const auto best_plan_left_iter = best_plan.find(csg_cmp_pair.first);
    const auto best_plan_right_iter = best_plan.find(csg_cmp_pair.second);
    DebugAssert(best_plan_left_iter != best_plan.end() && best_plan_right_iter != best_plan.end(),
                "Subplan missing: either the JoinGraph is invalid or EnumerateCcp is buggy.");

    duration52 += pair_timer.lap();

    const auto join_predicates = join_graph.find_join_predicates(csg_cmp_pair.first, csg_cmp_pair.second);
    duration53 += pair_timer.lap();

    // Experiments for #2626 showed that this is the major bottleneck as there are three calls to the cost and
    // cardinality estimator.
    auto candidate_plan =
        _add_join_to_plan(best_plan_left_iter->second, best_plan_right_iter->second, join_predicates, cost_estimator);
    duration54 += pair_timer.lap();

    const auto joined_vertex_set = csg_cmp_pair.first | csg_cmp_pair.second;
    duration55 += pair_timer.lap();

    const auto best_plan_iter = best_plan.find(joined_vertex_set);

    const auto not_init = best_plan_iter == best_plan.end();
    duration56 += pair_timer.lap();
    // The following two calls to the cost estimator cause the second bottleneck.
    auto est_timer = Timer{};
    const auto candidate_cost = not_init ? 0.0f : cost_estimator->estimate_plan_cost(candidate_plan);
    duration571 += est_timer.lap();
    const auto best_cost = not_init ? 0.0f : cost_estimator->estimate_plan_cost(best_plan_iter->second);
    duration572 += est_timer.lap();
    const auto is_cheaper = best_plan_iter == best_plan.end() ? true : (candidate_cost < best_cost);
    duration57 += pair_timer.lap();
    if (not_init || is_cheaper) {
    // The following two calls to the cost estimator cause the second bottleneck.
    //if (best_plan_iter == best_plan.end() || cost_estimator->estimate_plan_cost(candidate_plan) <
    //                                             cost_estimator->estimate_plan_cost(best_plan_iter->second)) {
      best_plan.insert_or_assign(joined_vertex_set, candidate_plan);
    }
    duration58 += pair_timer.lap();
  }
  const auto  duration5 = timer.lap();

  /**
   * 6. Build vertex set with all vertices and return the plan for it - this will be the best plan for the entire join
   *    graph.
   */
  auto all_vertices_set = JoinGraphVertexSet{vertex_count};
  all_vertices_set.flip();  // Turns all bits to '1'.

  const auto best_plan_iter = best_plan.find(all_vertices_set);
  Assert(best_plan_iter != best_plan.end(), "No plan for all vertices generated. Maybe JoinGraph is not connected?");
  const auto duration6 = timer.lap();

  std::cout << "    DpCcp: " << overall_timer.lap_formatted() << " (" << csg_cmp_pairs.size() << " candidates)" "\n";
  std::cout << "         1: " << format_duration(duration1) << "\n"
            << "       2.1: " << format_duration(duration21) << "\n"
            << "       2.2: " << format_duration(duration22) << "\n"
            << "         3: " << format_duration(duration3) << "\n"
            << "         4: " << format_duration(duration4) << "\n"
            << "         5: " << format_duration(duration5) << "\n"
            << "             5.1: " << format_duration(duration51) << "\n"
            << "             5.2: " << format_duration(duration52) << "\n"
            << "             5.3: " << format_duration(duration53) << "\n"
            << "             5.4: " << format_duration(duration54) << "\n"
            << "             5.5: " << format_duration(duration55) << "\n"
            << "             5.6: " << format_duration(duration56) << "\n"
            << "             5.7: " << format_duration(duration57) << "\n"
            << "                 5.7.1: " << format_duration(duration571) << "\n"
            << "                 5.7.2: " << format_duration(duration572) << "\n"
            << "             5.8: " << format_duration(duration58) << "\n"
            << "         6: " << format_duration(duration6) << "\n";
  const auto& ce = static_cast<const CardinalityEstimator&>(*(cost_estimator->cardinality_estimator));
  std::cout << "    Cardinality estimator: " << ce.bitmask_cache_hits << " / " << ce.global_cache_hits << " / "
            << ce.local_cache_hits << " / " << ce.cache_misses
            << "    " << format_duration(ce.bitmask_cache_time) <<  " / " << format_duration(ce.global_cache_time)
            << " / " << format_duration(ce.local_cache_time) << " / " << format_duration(ce.estimation_time)
            << " / " << format_duration(ce.caching_time)
            << "    " << format_duration(ce.cardinality_time) << " (" << format_duration(ce.cardinality_time2) << ") " << " / " << format_duration(ce.join_histogram_time) << "\n";

  for (const auto [type, time]: ce.time_by_node) {
    std::cout << "        " << magic_enum::enum_name(type) << " " << format_duration(time) << "\n";
  }

  return best_plan_iter->second;
}

}  // namespace hyrise
