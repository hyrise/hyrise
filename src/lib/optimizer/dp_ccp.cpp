#include "dp_ccp.hpp"

#include <unordered_map>

#include "cost_model/abstract_cost_model.hpp"
#include "enumerate_ccp.hpp"
#include "join_graph.hpp"
#include "logical_query_plan/lqp_utils.hpp"
#include "operators/operator_join_predicate.hpp"

namespace opossum {

DpCcp::DpCcp(const std::shared_ptr<AbstractCostModel>& cost_model) : _cost_model(cost_model) {}

std::shared_ptr<AbstractLQPNode> DpCcp::operator()(const JoinGraph& join_graph) {
  // No std::unordered_map, since hashing of JoinGraphVertexSet is not trivially possible because of hidden data
  auto best_plan = std::map<JoinGraphVertexSet, std::shared_ptr<AbstractLQPNode>>{};

  /**
   * 1. Initialize single-vertex vertex_sets with the vertex nodes and their local predicates
   */
  for (size_t vertex_idx = 0; vertex_idx < join_graph.vertices.size(); ++vertex_idx) {
    const auto vertex_predicates = join_graph.find_local_predicates(vertex_idx);
    const auto vertex = join_graph.vertices[vertex_idx];

    auto single_vertex_set = JoinGraphVertexSet{join_graph.vertices.size()};
    single_vertex_set.set(vertex_idx);

    best_plan[single_vertex_set] = _add_predicates(vertex, vertex_predicates);
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
   * 3. Actual DpCcp algorithm: Enumerate the CsgCmpPairs; build candidate plans; update best_plan
   */
  const auto csg_cmp_pairs = EnumerateCcp{join_graph.vertices.size(), enumerate_ccp_edges}();  // NOLINT
  for (const auto& csg_cmp_pair : csg_cmp_pairs) {
    const auto best_plan_left_iter = best_plan.find(csg_cmp_pair.first);
    const auto best_plan_right_iter = best_plan.find(csg_cmp_pair.second);
    DebugAssert(best_plan_left_iter != best_plan.end() && best_plan_right_iter != best_plan.end(),
                "Subplan missing: either the JoinGraph is invalid or EnumerateCcp is buggy");

    const auto join_predicates = join_graph.find_join_predicates(csg_cmp_pair.first, csg_cmp_pair.second);

    auto candidate_plan = _join(best_plan_left_iter->second, best_plan_right_iter->second, join_predicates);

    const auto joined_vertex_set = csg_cmp_pair.first | csg_cmp_pair.second;

    const auto best_plan_iter = best_plan.find(joined_vertex_set);
    if (best_plan_iter == best_plan.end() ||
        _cost_model->estimate_plan_cost(candidate_plan) < _cost_model->estimate_plan_cost(best_plan_iter->second)) {
      best_plan.insert_or_assign(joined_vertex_set, candidate_plan);
    }
  }

  /**
   * 4. Build vertex set with all vertices and return the plan for it
   */
  boost::dynamic_bitset<> all_vertices_set{join_graph.vertices.size()};
  all_vertices_set.flip();  // Turns all bits to '1'

  const auto best_plan_iter = best_plan.find(all_vertices_set);
  Assert(best_plan_iter != best_plan.end(), "No plan for all vertices generated. Maybe JoinGraph isn't connected?");

  return best_plan_iter->second;
}

std::shared_ptr<AbstractLQPNode> DpCcp::_add_predicates(
    const std::shared_ptr<AbstractLQPNode>& lqp,
    const std::vector<std::shared_ptr<AbstractExpression>>& predicates) const {
  if (predicates.empty()) return lqp;

  /**
   * The optimality-ensuring way to sort the scan operations would be to find the cheapest of the predicates.size()!
   * orders of them.
   * For now, we just execute the scan operations in the order of increasing cost that they would have when executed
   * directly on top of `lqp`
   */

  auto predicate_nodes_and_cost = std::vector<std::pair<std::shared_ptr<AbstractLQPNode>, Cost>>{};
  predicate_nodes_and_cost.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    const auto predicate_node = PredicateNode::make(predicate, lqp);
    predicate_nodes_and_cost.emplace_back(predicate_node, _cost_model->estimate_plan_cost(predicate_node));
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

std::shared_ptr<AbstractLQPNode> DpCcp::_join(const std::shared_ptr<AbstractLQPNode>& left_lqp,
                                              const std::shared_ptr<AbstractLQPNode>& right_lqp,
                                              std::vector<std::shared_ptr<AbstractExpression>> predicates) const {
  if (predicates.empty()) return JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);

  /**
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

  // Sort the predicates by increasing cost
  auto predicates_and_cost = std::vector<std::pair<std::shared_ptr<AbstractExpression>, Cost>>{};
  predicates_and_cost.reserve(predicates.size());
  for (const auto& predicate : predicates) {
    const auto join_node = JoinNode::make(JoinMode::Inner, predicate, left_lqp, right_lqp);
    predicates_and_cost.emplace_back(predicate, _cost_model->estimate_plan_cost(join_node));

    // need to do this since nodes do not get properly (by design :(( ) removed from plan on their destruction
    join_node->set_left_input(nullptr);
    join_node->set_right_input(nullptr);
  }

  std::sort(predicates_and_cost.begin(), predicates_and_cost.end(),
            [&](const auto& lhs, const auto& rhs) { return lhs.second < rhs.second; });

  // Find the simple predicate with the lowest cost (if any exists), which will act as the primary predicate
  std::shared_ptr<AbstractExpression> primary_predicate;
  for (auto predicate_iter = predicates_and_cost.begin(); predicate_iter != predicates_and_cost.end();
       ++predicate_iter) {
    const auto operator_join_predicate =
        OperatorJoinPredicate::from_expression(*predicate_iter->first, *left_lqp, *right_lqp);
    if (operator_join_predicate) {
      primary_predicate = predicate_iter->first;
      predicates_and_cost.erase(predicate_iter);
      break;
    }
  }

  // Build JoinNode (for primary predicate) and subsequent scans (for secondary predicates)
  auto lqp = std::shared_ptr<AbstractLQPNode>{};
  if (primary_predicate) {
    lqp = JoinNode::make(JoinMode::Inner, primary_predicate, left_lqp, right_lqp);
  } else {
    lqp = JoinNode::make(JoinMode::Cross, left_lqp, right_lqp);
  }

  for (const auto& predicate_and_cost : predicates_and_cost) {
    lqp = PredicateNode::make(predicate_and_cost.first, lqp);
  }

  return lqp;
}

}  // namespace opossum
