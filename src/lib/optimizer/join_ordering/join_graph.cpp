#include "join_graph.hpp"

#include "utils/assert.hpp"

#include "join_graph_builder.hpp"

namespace {

using namespace opossum;  // NOLINT

void build_all_in_lqp_impl(const std::shared_ptr<AbstractLQPNode>& lqp, std::vector<JoinGraph>& join_graphs,
                           std::unordered_set<std::shared_ptr<AbstractLQPNode>>& visited_nodes) {
  if (!lqp) return;
  if (!visited_nodes.emplace(lqp).second) return;

  const auto join_graph = JoinGraph::build_from_lqp(lqp);
  if (join_graph) {
    join_graphs.emplace_back(*join_graph);

    // A JoinGraph could be built from the subgraph rooted at `lqp`. Continue the search for more JoinGraphs from
    // the JoinGraph's vertices inputs.
    for (const auto& vertex : join_graph->vertices) {
      build_all_in_lqp_impl(vertex->left_input(), join_graphs, visited_nodes);
      build_all_in_lqp_impl(vertex->right_input(), join_graphs, visited_nodes);
    }
  } else {
    // No JoinGraph could be built from the subgraph rooted at `lqp`. Continue the search from the inputs of `lqp`.
    build_all_in_lqp_impl(lqp->left_input(), join_graphs, visited_nodes);
    build_all_in_lqp_impl(lqp->right_input(), join_graphs, visited_nodes);
  }
}

}  // namespace

namespace opossum {

std::optional<JoinGraph> JoinGraph::build_from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return JoinGraphBuilder{}(lqp);  // NOLINT - doesn't like {} followed by ()
}

std::vector<JoinGraph> JoinGraph::build_all_in_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  auto join_graphs = std::vector<JoinGraph>{};
  auto visited_nodes = std::unordered_set<std::shared_ptr<AbstractLQPNode>>{};
  build_all_in_lqp_impl(lqp, join_graphs, visited_nodes);
  return join_graphs;
}

JoinGraph::JoinGraph(const std::vector<std::shared_ptr<AbstractLQPNode>>& vertices,
                     const std::vector<JoinGraphEdge>& edges)
    : vertices(vertices), edges(edges) {}

std::vector<std::shared_ptr<AbstractExpression>> JoinGraph::find_local_predicates(const size_t vertex_idx) const {
  std::vector<std::shared_ptr<AbstractExpression>> predicates;

  auto vertex_set = JoinGraphVertexSet{vertices.size()};
  vertex_set.set(vertex_idx);

  for (const auto& edge : edges) {
    if (edge.vertex_set != vertex_set) continue;

    for (const auto& predicate : edge.predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::vector<std::shared_ptr<AbstractExpression>> JoinGraph::find_join_predicates(
    const JoinGraphVertexSet& vertex_set_a, const JoinGraphVertexSet& vertex_set_b) const {
  DebugAssert((vertex_set_a & vertex_set_b).none(), "Vertex sets are not distinct");

  std::vector<std::shared_ptr<AbstractExpression>> predicates;

  for (const auto& edge : edges) {
    if ((edge.vertex_set & vertex_set_a).none() || (edge.vertex_set & vertex_set_b).none()) continue;
    if (!edge.vertex_set.is_subset_of(vertex_set_a | vertex_set_b)) continue;

    for (const auto& predicate : edge.predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

void JoinGraph::print(std::ostream& stream) const {
  stream << "==== Vertices ====" << std::endl;
  if (vertices.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& vertex : vertices) {
      stream << vertex->description() << std::endl;
    }
  }
  stream << "===== Edges ======" << std::endl;
  if (edges.empty()) {
    stream << "<none>" << std::endl;
  } else {
    for (const auto& edge : edges) {
      edge.print(stream);
    }
  }
  std::cout << std::endl;
}

}  // namespace opossum
