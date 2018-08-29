#include "join_graph.hpp"

#include "utils/assert.hpp"

#include "join_graph_builder.hpp"

namespace opossum {

std::optional<JoinGraph> JoinGraph::from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return JoinGraphBuilder{}(lqp);  // NOLINT - doesn't like {} followed by ()
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
