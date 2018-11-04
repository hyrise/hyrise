#include "dp_ccp.hpp"

#include <unordered_map>

#include "cost_model/abstract_cost_estimator.hpp"
#include "enumerate_ccp.hpp"
#include "join_graph.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"

namespace opossum {

DpCcp::DpCcp(const std::shared_ptr<AbstractCostEstimator>& cost_estimator) : _cost_estimator(cost_estimator) {}

std::shared_ptr<AbstractLQPNode> DpCcp::operator()(const JoinGraph& join_graph) {
  // No std::unordered_map, since hashing of JoinGraphVertexSet is not (efficiently) possible because
  // boost::dynamic_bitset hides the data necessary for doing so efficiently.
  auto best_plan = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};

  /**
   * 1. Initialize single-vertex vertex_sets with the vertex nodes and their local predicates
   */
  for (size_t vertex_idx = 0; vertex_idx < join_graph.vertices.size(); ++vertex_idx) {
    const auto vertex_predicates = join_graph.find_local_predicates(vertex_idx);
    const auto vertex = join_graph.vertices[vertex_idx];

    auto single_vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    single_vertex_set.set(vertex_idx);

    best_plan[single_vertex_set] = _add_predicates_to_plan(vertex, vertex_predicates);
  }

  /**
   * 2. Prepare EnumerateCcp: Transform the JoinGraph's vertex-to-vertex edges into index pairs
   */
  std::vector<std::pair<size_t, size_t>> enumerate_ccp_edges;
  for (const auto& edge : join_graph.edges) {
    // Don't include local predicate pseudo edges
    if (edge.vertex_set.count() == 1) continue;
    Assert(edge.vertex_set.count() == 2, "Can't place complex predicates yet");

    const auto first_vertex_idx = edge.vertex_set.find_first();
    const auto second_vertex_idx = edge.vertex_set.find_next(first_vertex_idx);

    enumerate_ccp_edges.emplace_back(first_vertex_idx, second_vertex_idx);
  }

  /**
   * 3. Actual DpCcp algorithm: Enumerate the CsgCmpPairs; build candidate plans; update best_plan if the candidate plan
   *                            is cheaper than the cheapest currently known plan for a particular subset of vertices.
   */
  const auto csg_cmp_pairs = EnumerateCcp{join_graph.vertices.size(), enumerate_ccp_edges}();  // NOLINT
  for (const auto& csg_cmp_pair : csg_cmp_pairs) {
    const auto best_plan_left_iter = best_plan.find(csg_cmp_pair.first);
    const auto best_plan_right_iter = best_plan.find(csg_cmp_pair.second);
    DebugAssert(best_plan_left_iter != best_plan.end() && best_plan_right_iter != best_plan.end(),
                "Subplan missing: either the JoinGraph is invalid or EnumerateCcp is buggy");

    const auto join_predicates = join_graph.find_join_predicates(csg_cmp_pair.first, csg_cmp_pair.second);

    auto candidate_plan = _add_join_to_plan(best_plan_left_iter->second, best_plan_right_iter->second, join_predicates);

    const auto joined_vertex_set = csg_cmp_pair.first | csg_cmp_pair.second;

    const auto best_plan_iter = best_plan.find(joined_vertex_set);
    if (best_plan_iter == best_plan.end() || _cost_estimator->estimate_plan_cost(candidate_plan) <
                                                 _cost_estimator->estimate_plan_cost(best_plan_iter->second)) {
      best_plan.insert_or_assign(joined_vertex_set, candidate_plan);
    }
  }

  /**
   * 4. Build vertex set with all vertices and return the plan for it - this will be the best plan for the entire join
   *    graph.
   */
  boost::dynamic_bitset<> all_vertices_set{join_graph.vertices.size()};
  all_vertices_set.flip();  // Turns all bits to '1'

  const auto best_plan_iter = best_plan.find(all_vertices_set);
  Assert(best_plan_iter != best_plan.end(), "No plan for all vertices generated. Maybe JoinGraph isn't connected?");

  return best_plan_iter->second;
}

std::shared_ptr<AbstractLQPNode> DpCcp::_add_predicates_to_plan(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::vector<std::shared_ptr<AbstractExpression>>& predicates) const {
  /**
   * Add a number of predicates on top of a plan; try to bring them into an efficient order
   *
   *
   * The optimality-ensuring way to sort the scan operations would be to find the cheapest of the predicates.size()!
   * orders of them.
   * For now, we just execute the scan operations in the order of increasing cost that they would have when executed
   * directly on top of `lqp`
   */

  if (predicates.empty()) return lqp;

  auto predicate_nodes_and_cost = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cost>>{};
  predicate_nodes_and_cost.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    const auto predicate_node = PredicateNode::make(predicate, lqp);
    predicate_nodes_and_cost.emplace_back(predicate_node, _cost_estimator->estimate_plan_cost(predicate_node));
  }

  std::sort(predicate_nodes_and_cost.begin(), predicate_nodes_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  predicate_nodes_and_cost.front().first->set_left_input(lqp);

  for (auto predicate_node_idx = size_t{1}; predicate_node_idx < predicate_nodes_and_cost.size();
       ++predicate_node_idx) {
    predicate_nodes_and_cost[predicate_node_idx].first->set_left_input(
        predicate_nodes_and_cost[predicate_node_idx - 1].first);
  }

  return predicate_nodes_and_cost.back().first;
}

std::shared_ptr<AbstractLQPNode> DpCcp::_add_join_to_plan(
    const std::shared_ptr<AbstractLQPNode>& left_lqp, const std::shared_ptr<AbstractLQPNode>& right_lqp,
    std::vector<std::shared_ptr<AbstractExpression>> join_predicates) const {
  /**
   * Join two plans using a set of predicates; try to bring them into an efficient order
   *
   *
   * One predicate ("primary predicate") becomes the join predicate, the others ("secondary predicates) are executed as
   * column-to-column scans after the join.
   * The primary predicate needs to be a simple "<column> <operator> <column>" predicate, otherwise the join operators
   * won't be able to execute it.
   *
   * The optimality-ensuring way to order the predicates would be to find the cheapest of the predicates.size()!
   * orders of them.
   * For now, we just execute the scan operations in the order of increasing cost that they would have when executed
   * directly on top of `lqp`, with the cheapest predicate becoming the primary predicate.
   */

  if (join_predicates.empty()) return JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);

  // Sort the predicates by increasing cost
  auto join_predicates_and_cost = std::vector<std::pair<std::shared_ptr<AbstractExpression>, Cost>>{};
  join_predicates_and_cost.reserve(join_predicates.size());
  for (const auto& join_predicate : join_predicates) {
    const auto join_node = JoinNode::make(JoinMode::Inner, join_predicate, left_lqp, right_lqp);
    join_predicates_and_cost.emplace_back(join_predicate, _cost_estimator->estimate_plan_cost(join_node));

    // need to do this since nodes do not get properly (by design :(( ) removed from plan on their destruction
    join_node->set_left_input(nullptr);
    join_node->set_right_input(nullptr);
  }

  std::sort(join_predicates_and_cost.begin(), join_predicates_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  // Find the simple predicate with the lowest cost (if any exists), which will act as the primary predicate
  auto primary_join_predicate = std::shared_ptr<AbstractExpression>{};
  for (auto predicate_iter = join_predicates_and_cost.begin(); predicate_iter != join_predicates_and_cost.end();
       ++predicate_iter) {
    // If a predicate can be converted into an OperatorJoinPredicate, it can be used as a primary predicate
    const auto operator_join_predicate =
        OperatorJoinPredicate::from_expression(*predicate_iter->first, *left_lqp, *right_lqp);
    if (operator_join_predicate) {
      primary_join_predicate = predicate_iter->first;
      join_predicates_and_cost.erase(predicate_iter);
      break;
    }
  }

  // Build JoinNode (for primary predicate) and subsequent scans (for secondary predicates)
  auto lqp = std::shared_ptr<AbstractLQPNode>{};
  if (primary_join_predicate) {
    lqp = JoinNode::make(JoinMode::Inner, primary_join_predicate, left_lqp, right_lqp);
  } else {
    lqp = JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);
  }

  for (const auto& predicate_and_cost : join_predicates_and_cost) {
    lqp = PredicateNode::make(predicate_and_cost.first, lqp);
  }

  return lqp;
}

}  // namespace opossum
