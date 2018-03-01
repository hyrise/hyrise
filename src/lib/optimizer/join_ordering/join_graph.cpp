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

std::shared_ptr<JoinGraph> JoinGraph::from_predicates(
    std::vector<std::shared_ptr<AbstractLQPNode>> vertices, std::vector<LQPOutputRelation> output_relations,
    const std::vector<std::shared_ptr<const AbstractJoinPlanPredicate>>& predicates) {
  std::unordered_map<std::shared_ptr<AbstractLQPNode>, size_t> vertex_to_index;
  std::map<JoinVertexSet, std::shared_ptr<JoinEdge>> vertices_to_edge;
  std::vector<std::shared_ptr<JoinEdge>> edges;

  for (size_t vertex_idx = 0; vertex_idx < vertices.size(); ++vertex_idx) {
    vertex_to_index[vertices[vertex_idx]] = vertex_idx;
  }

  for (const auto& predicate : predicates) {
    const auto vertex_set = predicate->get_accessed_vertex_set(vertices);
    auto iter = vertices_to_edge.find(vertex_set);
    if (iter == vertices_to_edge.end()) {
      auto edge = std::make_shared<JoinEdge>(vertex_set);
      iter = vertices_to_edge.emplace(vertex_set, edge).first;
      edges.emplace_back(edge);
    }
    iter->second->predicates.emplace_back(predicate);
  }

  return std::make_shared<JoinGraph>(std::move(vertices), std::move(output_relations), std::move(edges));
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
