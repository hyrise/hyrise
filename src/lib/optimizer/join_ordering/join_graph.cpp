#include "join_graph.hpp"

#include <memory>
#include <optional>
#include <utility>
#include <vector>

#include "constant_mappings.hpp"
#include "join_edge.hpp"
#include "join_graph_builder.hpp"
#include "join_plan_predicate.hpp"
#include "types.hpp"
#include "utils/assert.hpp"

namespace opossum {

std::shared_ptr<JoinGraph> JoinGraph::from_lqp(const std::shared_ptr<AbstractLQPNode>& lqp) {
  return JoinGraphBuilder{}(lqp);  // NOLINT - doesn't like {} followed by ()
}

JoinGraph::JoinGraph(std::vector<std::shared_ptr<AbstractLQPNode>> vertices,
                     std::vector<LQPOutputRelation> output_relations, std::vector<std::shared_ptr<JoinEdge>> edges)
    : vertices(std::move(vertices)), output_relations(std::move(output_relations)), edges(std::move(edges)) {}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const JoinVertexSet& vertex_set) const {
  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if (edge->vertex_set != vertex_set) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> JoinGraph::find_predicates(
    const JoinVertexSet& vertex_set_a, const JoinVertexSet& vertex_set_b) const {
  DebugAssert((vertex_set_a & vertex_set_b).none(), "Vertex sets are not distinct");

  std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>> predicates;

  for (const auto& edge : edges) {
    if ((edge->vertex_set & vertex_set_a).none() || (edge->vertex_set & vertex_set_b).none()) continue;
    if (!edge->vertex_set.is_subset_of(vertex_set_a | vertex_set_b)) continue;

    for (const auto& predicate : edge->predicates) {
      predicates.emplace_back(predicate);
    }
  }

  return predicates;
}

std::shared_ptr<JoinEdge> JoinGraph::find_edge(const JoinVertexSet& vertex_set) const {
  const auto iter =
      std::find_if(edges.begin(), edges.end(), [&](const auto& edge) { return edge->vertex_set == vertex_set; });
  return iter == edges.end() ? nullptr : *iter;
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
      edge->print(stream);
    }
  }
  std::cout << std::endl;
}

}  // namespace opossum
